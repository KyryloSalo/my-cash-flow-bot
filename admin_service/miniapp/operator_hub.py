from __future__ import annotations

from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.urls import reverse
from django.utils import timezone

from bot_events.models import BotEvent
from broadcasts.models import AdminMessageLog, Broadcast, BroadcastRecipient
from broadcasts.tasks import send_manual_message_task
from common.audit import create_audit_log
from dashboard import services as dashboard_services
from polls.models import PollRecipient
from subscriptions.billing import (
    get_latest_refundable_monobank_payment,
    refund_latest_monobank_payment,
    retry_monobank_charge,
)
from subscriptions.models import BillingProfile, Payment
from subscriptions.monobank import MonobankAPIError
from subscriptions.services import apply_subscription_change, latest_subscription_for_user, resolve_subscription_plan_ref
from support.models import SupportCase, SupportMessage
from support.tasks import SUPPORT_DELIVERY_UNCERTAIN
from users.models import AdminNote, Tag, TelegramUser, UserAdminState, UserTag
from users.services import (
    HardDeleteUserError,
    ResetOnboardingError,
    hard_delete_user,
    mark_user_as_test_user,
    reset_user_onboarding,
    unmark_user_as_test_user,
)


class OperatorUserError(Exception):
    def __init__(self, code: str, message: str, *, status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def operator_telegram_ids() -> set[int]:
    result: set[int] = set()
    for value in getattr(settings, "MINIAPP_OPERATOR_TELEGRAM_IDS", []):
        try:
            tg_user_id = int(value)
        except (TypeError, ValueError):
            continue
        if tg_user_id > 0:
            result.add(tg_user_id)
    return result


def is_operator_user(user: TelegramUser | object) -> bool:
    try:
        tg_user_id = int(getattr(user, "tg_user_id", 0) or 0)
    except (TypeError, ValueError):
        return False
    return tg_user_id in operator_telegram_ids()


def _admin_url(name: str, *args) -> str:
    domain = str(getattr(settings, "ADMIN_DOMAIN", "admin.vydno.capital") or "admin.vydno.capital").strip()
    return f"https://{domain}{reverse(name, args=args)}"


def _admin_state(user: TelegramUser):
    try:
        return user.admin_state
    except UserAdminState.DoesNotExist:
        return None


def _iso(value) -> str:
    return value.isoformat() if value else ""


def _operator_mode(operator_id: int, action: str) -> str:
    return f"miniapp_operator:{operator_id}:{action}"


def _require_operator(operator: TelegramUser | object) -> int:
    try:
        operator_id = int(getattr(operator, "tg_user_id", 0) or 0)
    except (TypeError, ValueError):
        operator_id = 0
    if operator_id not in operator_telegram_ids():
        raise OperatorUserError("operator_access_denied", "Operator access denied.", status=403)
    return operator_id


def _target_user(tg_user_id: int, *, lock: bool = False) -> TelegramUser:
    try:
        target_id = int(tg_user_id)
    except (TypeError, ValueError):
        raise OperatorUserError("invalid_user", "Некоректний Telegram ID.")
    queryset = TelegramUser.objects.select_related("admin_state")
    if lock:
        queryset = queryset.select_for_update()
    try:
        return queryset.get(tg_user_id=target_id)
    except TelegramUser.DoesNotExist:
        raise OperatorUserError("user_not_found", "Користувача не знайдено.", status=404)


def _positive_days(value, *, default: int) -> int:
    try:
        days = int(value or default)
    except (TypeError, ValueError):
        raise OperatorUserError("invalid_days", "Кількість днів має бути цілим числом.")
    if not 1 <= days <= 3650:
        raise OperatorUserError("invalid_days", "Вкажіть від 1 до 3650 днів.")
    return days


def _user_payload(user: TelegramUser) -> dict[str, object]:
    state = _admin_state(user)
    username = str(user.username or "").strip().lstrip("@")
    return {
        "tg_user_id": int(user.tg_user_id),
        "name": user.full_name,
        "username": f"@{username}" if username else "",
        "last_seen_at": user.last_seen_at.isoformat() if user.last_seen_at else "",
        "onboarding_completed": bool(user.onboarding_completed),
        "status": str(state.status if state else UserAdminState.Status.ACTIVE),
        "subscription_status": str(
            state.subscription_status if state else UserAdminState.SubscriptionStatus.NONE
        ),
        "is_blocked": bool(state and (state.is_blocked or state.status == UserAdminState.Status.BANNED)),
        "can_receive_messages": bool(not state or (state.can_receive_messages and not state.blocked_bot)),
    }


def build_operator_user_detail(tg_user_id: int) -> dict[str, object]:
    try:
        user = TelegramUser.objects.select_related("admin_state").get(tg_user_id=int(tg_user_id))
    except (TelegramUser.DoesNotExist, TypeError, ValueError):
        raise OperatorUserError("user_not_found", "Користувача не знайдено.", status=404)

    state = _admin_state(user)
    subscription = latest_subscription_for_user(user)
    profile = BillingProfile.objects.filter(user_id=user.tg_user_id).first()
    refundable_payment = get_latest_refundable_monobank_payment(user_id=user.tg_user_id)
    tags = list(
        UserTag.objects.filter(user_id=user.tg_user_id)
        .select_related("tag")
        .order_by("tag__name")
        .values("tag_id", "tag__name")
    )
    notes = list(
        AdminNote.objects.filter(telegram_user_id=user.tg_user_id)
        .order_by("-created_at")
        .values("id", "note_text", "created_at")[:5]
    )
    payload = _user_payload(user)
    payload.update(
        {
            "created_at": user.created_at.isoformat() if user.created_at else "",
            "base_currency": str(user.base_currency or ""),
            "lang": str(user.lang or ""),
            "access_scope": str(state.access_scope if state else UserAdminState.AccessScope.PAYWALL),
            "access_source": str(state.access_source if state else ""),
            "blocked_bot": bool(state and state.blocked_bot),
            "admin_comment": str(state.admin_comment if state else ""),
            "is_test_user": bool(state and state.is_test_user),
            "test_user_notes": str(state.test_user_notes if state else ""),
            "is_operator": int(user.tg_user_id) in operator_telegram_ids(),
            "subscription": {
                "status": str(subscription.status if subscription else ""),
                "plan": str(subscription.plan if subscription else ""),
                "expires_at": _iso(subscription.expires_at if subscription else None),
                "next_charge_at": _iso(subscription.next_charge_at if subscription else None),
                "grace_expires_at": _iso(subscription.grace_expires_at if subscription else None),
                "auto_renew": bool(subscription and subscription.auto_renew),
            },
            "billing": {
                "has_card": bool(profile and profile.card_token),
                "masked_pan": str(profile.masked_pan if profile else ""),
                "auto_renew_enabled": bool(profile and profile.auto_renew_enabled),
                "last_charge_status": str(profile.last_charge_status if profile else ""),
                "last_failure_reason": str(profile.last_failure_reason if profile else ""),
                "can_force_charge": bool(profile and profile.card_token and profile.auto_renew_enabled),
                "can_refund": bool(refundable_payment),
                "refundable_payment": (
                    {
                        "id": refundable_payment.pk,
                        "amount": str(refundable_payment.amount),
                        "currency": str(refundable_payment.currency),
                    }
                    if refundable_payment
                    else None
                ),
            },
            "tags": [{"id": row["tag_id"], "name": row["tag__name"]} for row in tags],
            "notes": [
                {"id": row["id"], "text": row["note_text"], "created_at": _iso(row["created_at"])}
                for row in notes
            ],
        }
    )
    return payload


def _audit_operator_action(
    *,
    request,
    operator_id: int,
    action: str,
    target: TelegramUser,
    reason: str = "",
    object_type: str = "telegram_user",
    object_id=None,
    before=None,
    after=None,
) -> None:
    create_audit_log(
        request=request,
        admin_user=None,
        action=action,
        object_type=object_type,
        object_id=target.pk if object_id is None else object_id,
        target_user_id=target.tg_user_id,
        mode=_operator_mode(operator_id, action),
        reason=reason,
        before=before or {},
        after=after or {},
    )


def execute_operator_user_action(
    *,
    operator: TelegramUser | object,
    tg_user_id: int,
    action: str,
    payload: dict | None = None,
    request=None,
) -> dict[str, object]:
    operator_id = _require_operator(operator)
    data = dict(payload or {})
    normalized_action = str(action or "").strip().lower()
    reason = str(data.get("reason") or data.get("comment") or "").strip()[:1000]
    target = _target_user(tg_user_id)

    if target.tg_user_id in operator_telegram_ids() and normalized_action in {
        "ban",
        "erase_data",
        "hard_delete",
        "reset_onboarding",
    }:
        raise OperatorUserError("operator_is_protected", "Адміністратора не можна змінити цією дією.", status=409)

    if normalized_action in {"ban", "unban"}:
        user_payload = update_operator_user_status(
            operator=operator,
            tg_user_id=target.tg_user_id,
            action=normalized_action,
            reason=reason,
            request=request,
        )
        return {"message": "Статус користувача оновлено.", "user": user_payload}

    subscription_actions = {
        "subscription_trial": "trial",
        "subscription_extend": "manual",
        "subscription_expire": "expire",
        "subscription_cancel": "cancel",
    }
    if normalized_action in subscription_actions:
        service_action = subscription_actions[normalized_action]
        days = _positive_days(data.get("days"), default=30) if service_action in {"trial", "manual"} else None
        latest = latest_subscription_for_user(target)
        before = {
            "status": latest.status if latest else "",
            "expires_at": _iso(latest.expires_at if latest else None),
        }
        plan_ref = resolve_subscription_plan_ref(latest)
        subscription = apply_subscription_change(
            user=target,
            action=service_action,
            admin_user=None,
            plan_slug=plan_ref.slug if plan_ref else (latest.plan if latest else "solo"),
            plan_ref=plan_ref,
            days=days,
            comment=reason or f"Mobile operator {operator_id}",
        )
        _audit_operator_action(
            request=request,
            operator_id=operator_id,
            action="subscription_changed",
            target=target,
            reason=reason,
            object_type="subscription",
            object_id=subscription.pk,
            before=before,
            after={"status": subscription.status, "expires_at": _iso(subscription.expires_at), "days": days},
        )
        return {"message": "Дію з підпискою виконано.", "user": build_operator_user_detail(target.tg_user_id)}

    if normalized_action == "force_charge":
        profile = BillingProfile.objects.filter(user_id=target.tg_user_id).first()
        if profile is None or not profile.card_token:
            raise OperatorUserError("card_required", "У користувача немає збереженої картки.", status=409)
        if not profile.auto_renew_enabled:
            raise OperatorUserError("autorenew_disabled", "Автопродовження вимкнене.", status=409)
        try:
            result = retry_monobank_charge(user_id=target.tg_user_id)
        except (ValueError, MonobankAPIError) as exc:
            raise OperatorUserError("charge_failed", str(exc), status=409)
        _audit_operator_action(
            request=request,
            operator_id=operator_id,
            action="billing_force_charge_now",
            target=target,
            reason=reason,
            object_type="payment",
            object_id=result.get("payment_id"),
            after=result,
        )
        return {
            "message": "Списання запущено.",
            "result": result,
            "user": build_operator_user_detail(target.tg_user_id),
        }

    if normalized_action == "refund_latest":
        try:
            result = refund_latest_monobank_payment(user_id=target.tg_user_id, reason=reason, admin_user=None)
        except (ValueError, MonobankAPIError) as exc:
            raise OperatorUserError("refund_failed", str(exc), status=409)
        _audit_operator_action(
            request=request,
            operator_id=operator_id,
            action="billing_refund_latest_payment",
            target=target,
            reason=reason,
            object_type="payment",
            object_id=result.get("payment_id"),
            after=result,
        )
        return {
            "message": "Refund запущено. Автопродовження вимкнено.",
            "result": result,
            "user": build_operator_user_detail(target.tg_user_id),
        }

    if normalized_action == "send_message":
        message_text = str(data.get("message_text") or "").strip()
        if not message_text:
            raise OperatorUserError("message_required", "Введіть текст повідомлення.")
        if len(message_text) > 4000:
            raise OperatorUserError("message_too_long", "Повідомлення має містити не більше 4000 символів.")
        parse_mode = str(data.get("parse_mode") or Broadcast.ParseMode.NONE)
        if parse_mode not in dict(Broadcast.ParseMode.choices):
            raise OperatorUserError("invalid_parse_mode", "Некоректний формат повідомлення.")
        message_log = AdminMessageLog.objects.create(
            telegram_user_id=target.tg_user_id,
            admin_user=None,
            message_text=message_text,
            parse_mode=parse_mode,
            send_test_to_admin_first=False,
            target_chat_id=target.tg_user_id,
            response_payload={"operator_tg_user_id": operator_id},
        )
        _audit_operator_action(
            request=request,
            operator_id=operator_id,
            action="manual_message_queued",
            target=target,
            object_type="admin_message_log",
            object_id=message_log.pk,
            after={"message_log_id": message_log.pk},
        )
        transaction.on_commit(lambda: send_manual_message_task.delay(message_log.id, is_test=False))
        return {"message": "Повідомлення поставлено в чергу.", "user": build_operator_user_detail(target.tg_user_id)}

    if normalized_action == "add_note":
        note_text = str(data.get("note_text") or "").strip()
        if not note_text:
            raise OperatorUserError("note_required", "Введіть текст нотатки.")
        if len(note_text) > 4000:
            raise OperatorUserError("note_too_long", "Нотатка має містити не більше 4000 символів.")
        note = AdminNote.objects.create(telegram_user_id=target.tg_user_id, admin_user=None, note_text=note_text)
        _audit_operator_action(
            request=request,
            operator_id=operator_id,
            action="admin_note_added",
            target=target,
            object_id=target.pk,
            after={"note_id": note.pk},
        )
        return {"message": "Нотатку збережено.", "user": build_operator_user_detail(target.tg_user_id)}

    if normalized_action == "update_comment":
        state, _ = UserAdminState.objects.get_or_create(telegram_user_id=target.tg_user_id)
        before = {"admin_comment": state.admin_comment}
        state.admin_comment = str(data.get("admin_comment") or "").strip()[:4000]
        state.save(update_fields=["admin_comment", "updated_at"])
        _audit_operator_action(
            request=request,
            operator_id=operator_id,
            action="admin_comment_updated",
            target=target,
            reason=reason,
            before=before,
            after={"admin_comment": state.admin_comment},
        )
        return {"message": "Коментар оновлено.", "user": build_operator_user_detail(target.tg_user_id)}

    if normalized_action in {"mark_test", "unmark_test"}:
        if normalized_action == "mark_test":
            mark_user_as_test_user(
                user=target,
                admin_user=None,
                notes=str(data.get("test_user_notes") or "").strip()[:4000],
                request=request,
            )
            message = "Користувача позначено як тестового."
        else:
            unmark_user_as_test_user(user=target, admin_user=None, request=request)
            message = "Позначку тестового користувача знято."
        _audit_operator_action(
            request=request,
            operator_id=operator_id,
            action="test_user_marked_by_operator" if normalized_action == "mark_test" else "test_user_unmarked_by_operator",
            target=target,
            after={"is_test_user": normalized_action == "mark_test"},
        )
        return {"message": message, "user": build_operator_user_detail(target.tg_user_id)}

    if normalized_action in {"add_tag", "remove_tag"}:
        tag_name = " ".join(str(data.get("tag_name") or "").split()).strip()
        if not tag_name:
            raise OperatorUserError("tag_required", "Введіть назву тегу.")
        tag = Tag.objects.filter(name__iexact=tag_name).first()
        if normalized_action == "add_tag":
            tag = tag or Tag.objects.create(name=tag_name[:128])
            UserTag.objects.get_or_create(user_id=target.tg_user_id, tag=tag, defaults={"created_by": None})
            message = "Тег додано."
        else:
            if tag is None:
                raise OperatorUserError("tag_not_found", "Тег не знайдено.", status=404)
            UserTag.objects.filter(user_id=target.tg_user_id, tag=tag).delete()
            message = "Тег знято."
        _audit_operator_action(
            request=request,
            operator_id=operator_id,
            action=f"user_tag_{'assigned' if normalized_action == 'add_tag' else 'removed'}",
            target=target,
            object_type="tag",
            object_id=tag.pk,
            after={"tag": tag.name},
        )
        return {"message": message, "user": build_operator_user_detail(target.tg_user_id)}

    if normalized_action in {"reset_onboarding", "erase_data", "hard_delete"}:
        if not reason:
            raise OperatorUserError("reason_required", "Для цієї дії обов’язково вкажіть причину.")
        if normalized_action == "hard_delete":
            expected = f"DELETE USER {target.tg_user_id}"
            try:
                result = hard_delete_user(
                    target.tg_user_id,
                    admin_user=None,
                    reason=reason,
                    options={
                        "request": request,
                        "double_confirmed": True,
                        "confirmation_text": str(data.get("confirmation_text") or "").strip(),
                        "operator_authorized": True,
                        "operator_tg_user_id": operator_id,
                    },
                )
            except HardDeleteUserError as exc:
                raise OperatorUserError("hard_delete_failed", str(exc), status=409)
            return {"message": f"Користувача видалено. Підтвердження: {expected}", "deleted": True, "result": result}

        mode = "replay" if normalized_action == "erase_data" else str(data.get("mode") or "safe").strip().lower()
        confirmation_text = str(data.get("confirmation_text") or "").strip()
        try:
            result = reset_user_onboarding(
                target.tg_user_id,
                mode,
                None,
                reason,
                options={
                    "request": request,
                    "clear_fsm_state": bool(data.get("clear_fsm_state", True)),
                    "send_telegram_notice": bool(data.get("send_telegram_notice", True)),
                    "send_admin_notification": bool(data.get("send_admin_notification", True)),
                    "double_confirmed": True,
                    "confirmation_text": confirmation_text,
                    "operator_authorized": True,
                    "operator_tg_user_id": operator_id,
                },
            )
        except ResetOnboardingError as exc:
            raise OperatorUserError("reset_failed", str(exc), status=409)
        message = "Дані користувача стерто, доступ і billing збережено." if mode == "replay" else "Онбординг скинуто."
        return {"message": message, "result": result, "user": build_operator_user_detail(target.tg_user_id)}

    raise OperatorUserError("invalid_action", "Невідома дія адміністратора.")


@transaction.atomic
def update_operator_user_status(
    *,
    operator: TelegramUser | object,
    tg_user_id: int,
    action: str,
    reason: str = "",
    request=None,
) -> dict[str, object]:
    try:
        operator_id = int(getattr(operator, "tg_user_id", 0) or 0)
        target_id = int(tg_user_id)
    except (TypeError, ValueError):
        raise OperatorUserError("invalid_user", "Некоректний Telegram ID.")

    if operator_id not in operator_telegram_ids():
        raise OperatorUserError("operator_access_denied", "Operator access denied.", status=403)
    if target_id in operator_telegram_ids():
        raise OperatorUserError("operator_is_protected", "Адміністратора не можна заблокувати тут.", status=409)

    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"ban", "unban"}:
        raise OperatorUserError("invalid_action", "Доступні лише блокування або розблокування.")

    try:
        target = TelegramUser.objects.select_for_update().get(tg_user_id=target_id)
    except TelegramUser.DoesNotExist:
        raise OperatorUserError("user_not_found", "Користувача не знайдено.", status=404)

    state, _ = UserAdminState.objects.get_or_create(telegram_user_id=target_id)
    before = {"status": state.status, "is_blocked": state.is_blocked}
    is_banned = normalized_action == "ban"
    state.status = UserAdminState.Status.BANNED if is_banned else UserAdminState.Status.ACTIVE
    state.is_blocked = is_banned
    state.save(update_fields=["status", "is_blocked", "updated_at"])
    after = {"status": state.status, "is_blocked": state.is_blocked}

    create_audit_log(
        request=request,
        admin_user=None,
        action="user_banned" if is_banned else "user_unbanned",
        object_type="telegram_user",
        object_id=target.pk,
        target_user_id=target_id,
        mode=f"miniapp_operator:{operator_id}",
        reason=str(reason or "").strip()[:1000],
        before=before,
        after=after,
    )
    return build_operator_user_detail(target_id)


def _operator_users(query: str, *, limit: int = 12) -> list[dict[str, object]]:
    cleaned = str(query or "").strip().lstrip("@")
    queryset = TelegramUser.objects.select_related("admin_state")
    if cleaned:
        lookup = Q(username__icontains=cleaned) | Q(first_name__icontains=cleaned) | Q(last_name__icontains=cleaned)
        if cleaned.isdigit():
            lookup |= Q(tg_user_id=int(cleaned))
        queryset = queryset.filter(lookup)
    else:
        queryset = queryset.order_by("-last_seen_at", "-created_at")
    return [_user_payload(user) for user in queryset[:limit]]


def build_operator_payload(user: TelegramUser, *, query: str = "") -> dict[str, object]:
    now = timezone.now()
    today = timezone.localdate(now)
    snapshot = dashboard_services.build_current_user_metrics(include_test_users=False)

    # Same canonical real-user scope as KPI. Unattributed/system errors are
    # deliberately not user metrics; the system-health page remains separate.
    real_ids = dashboard_services._exclude_test_users(
        TelegramUser.objects.all(), prefix="", include_test_users=False,
    ).values_list("tg_user_id", flat=True)
    no_retry = "У роботі / результат невідомий; автоматичний повтор вимкнено"
    attention_queries = [
        ("failed_payments", "Проблемні платежі", Payment.objects.filter(user_id__in=real_ids, status__in=[Payment.Status.FAILED, Payment.Status.REJECTED], updated_at__date=today), "danger", "сьогодні · поточний статус платежу", "admin:subscriptions_payment_changelist"),
        ("bot_errors", "Помилки бота", BotEvent.objects.filter(user_id__in=real_ids, created_at__date=today, success=False), "danger", "сьогодні · події реальних користувачів", "admin:bot_events_botevent_changelist"),
        ("open_support", "Відкриті звернення", SupportCase.objects.filter(user_id__in=real_ids).exclude(status__in=[SupportCase.Status.RESOLVED, SupportCase.Status.CLOSED]), "warning", "відкриті зараз", "admin:support_supportcase_changelist"),
        ("broadcast_uncertain", "Невідома доставка розсилок", BroadcastRecipient.objects.filter(user_id__in=real_ids, status="uncertain"), "warning", no_retry, "admin:broadcasts_broadcastrecipient_changelist"),
        ("manual_uncertain", "Невідома доставка повідомлень", AdminMessageLog.objects.filter(telegram_user_id__in=real_ids, status="uncertain"), "warning", no_retry, "admin:broadcasts_adminmessagelog_changelist"),
        ("poll_uncertain", "Невідома доставка опитувань", PollRecipient.objects.filter(user_id__in=real_ids, status="uncertain"), "warning", no_retry, "admin:polls_pollrecipient_changelist"),
        ("support_uncertain", "Невідома доставка підтримки", SupportMessage.objects.filter(case__user_id__in=real_ids, sender_type=SupportMessage.SenderType.ADMIN, telegram_message_id=SUPPORT_DELIVERY_UNCERTAIN), "warning", no_retry, "admin:support_supportcase_changelist"),
    ]
    attention = []
    errors = []
    for key, label, queryset, tone, hint, url in attention_queries:
        value = dashboard_services.safe(queryset.count, errors=errors)
        attention.append({
            "id": key, "label": label, "value": value, "available": value is not None,
            "tone": "unknown" if value is None else tone if value else "ok",
            "hint": f"{snapshot['scope_label']} · {hint}",
            "url": _admin_url(url),
        })

    return {
        "operator": {
            "tg_user_id": int(user.tg_user_id),
            "name": user.full_name,
        },
        "metrics": snapshot["metrics"],
        "scope_label": snapshot["scope_label"],
        "retrieved_at": snapshot["retrieved_at"],
        "include_test_users": snapshot["include_test_users"],
        "degraded": snapshot["degraded"] or bool(errors),
        "attention": attention,
        "users": _operator_users(query),
        "query": str(query or "").strip(),
        "links": {
            "full_admin": _admin_url("admin:index"),
            "manual_message": _admin_url("admin:manual_message"),
            "broadcasts": _admin_url("admin:broadcasts_broadcast_changelist"),
            "subscriptions": _admin_url("admin:subscriptions_subscription_changelist"),
            "health": _admin_url("admin:system_health"),
        },
    }


def build_operator_preview_payload(*, query: str = "") -> dict[str, object]:
    users = [
        {
            "tg_user_id": 100100100,
            "name": "Олена Тестова",
            "username": "@olena_demo",
            "last_seen_at": timezone.now().isoformat(),
            "onboarding_completed": True,
            "status": "active",
            "subscription_status": "paid",
            "is_blocked": False,
            "can_receive_messages": True,
        },
        {
            "tg_user_id": 200200200,
            "name": "Demo User",
            "username": "@demo_user",
            "last_seen_at": "",
            "onboarding_completed": False,
            "status": "inactive",
            "subscription_status": "trial",
            "is_blocked": False,
            "can_receive_messages": False,
        },
    ]
    cleaned = str(query or "").strip().lower()
    if cleaned:
        users = [
            item
            for item in users
            if cleaned in str(item["name"]).lower()
            or cleaned in str(item["username"]).lower()
            or cleaned in str(item["tg_user_id"])
        ]
    return {
        "operator": {"tg_user_id": 7884326049, "name": "Askills Support"},
        "metrics": [
            {"label": "Користувачі", "value": 1842, "hint": "усього", "url": "https://admin.vydno.capital/"},
            {"label": "Активні", "value": 126, "hint": "за 24 години", "url": "https://admin.vydno.capital/"},
            {"label": "Платні", "value": 318, "hint": "активний доступ", "url": "https://admin.vydno.capital/"},
            {"label": "Не завершили", "value": 47, "hint": "онбординг", "url": "https://admin.vydno.capital/"},
        ],
        "attention": [
            {"label": "Проблемні платежі", "value": 3, "tone": "danger", "url": "https://admin.vydno.capital/"},
            {"label": "Помилки бота", "value": 1, "tone": "danger", "url": "https://admin.vydno.capital/"},
            {"label": "Відкриті звернення", "value": 8, "tone": "warning", "url": "https://admin.vydno.capital/"},
        ],
        "users": users,
        "query": str(query or "").strip(),
        "links": {
            "full_admin": "https://admin.vydno.capital/",
            "manual_message": "https://admin.vydno.capital/manual-message/",
            "broadcasts": "https://admin.vydno.capital/broadcasts/broadcast/",
            "subscriptions": "https://admin.vydno.capital/subscriptions/subscription/",
            "health": "https://admin.vydno.capital/healthcheck/",
        },
    }


def build_operator_preview_user_detail(tg_user_id: int) -> dict[str, object]:
    payload = build_operator_preview_payload()
    user = next((item for item in payload["users"] if int(item["tg_user_id"]) == int(tg_user_id)), None)
    if user is None:
        raise OperatorUserError("user_not_found", "Користувача не знайдено.", status=404)
    return {
        **user,
        "created_at": timezone.now().isoformat(),
        "base_currency": "UAH",
        "lang": "uk",
        "access_scope": "personal_full",
        "access_source": "preview",
        "blocked_bot": False,
        "admin_comment": "Тестова картка користувача",
        "is_operator": False,
    }
