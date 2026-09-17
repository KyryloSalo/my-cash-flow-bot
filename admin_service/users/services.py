from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from django.conf import settings
from django.db import connection, transaction
from django.utils import timezone

from accounts.models import Account, AccountAdminState
from admin_notifications.tasks import send_admin_notification_task
from bot_settings.models import BotSetting
from bot_events.models import BotEvent
from broadcasts.models import AdminMessageLog
from broadcasts.tasks import send_manual_message_task
from categories.models import Category
from common.audit import create_audit_log
from common.admin_actions import authenticated_operator_id, require_non_operator_target, require_service_action
from subscriptions.models import BillingProfile, Payment, PromoOfferClaim, Subscription, SubscriptionEvent
from transactions.models import Debt, DebtPayment, Transaction
from users.models import Tag, TelegramUser, UserAdminState, UserTag


RESET_MODES = (
    ("safe", "Скинути тільки онбординг"),
    ("cleanup", "Скинути онбординг і тестові дані"),
    ("full", "Повне скидання тестового користувача"),
)


REPLAY_RESET_CONFIRMATION = "ERASE USER DATA"


class ResetOnboardingError(Exception):
    pass


class HardDeleteUserError(Exception):
    pass


class SelfServiceAccountDeletionError(Exception):
    def __init__(self, code: str, message: str, *, status: int = 409):
        self.code = code
        self.status = status
        super().__init__(message)


_SELF_SERVICE_DELETE_CAPABILITY = object()


@dataclass(slots=True)
class CleanupStats:
    deactivated_accounts: int = 0
    deactivated_categories: int = 0
    deleted_accounts: int = 0
    deleted_account_admin_states: int = 0
    deleted_categories: int = 0
    deleted_transactions: int = 0
    deleted_debt_payments: int = 0
    deleted_debts: int = 0
    deleted_bot_events: int = 0
    deleted_admin_message_logs: int = 0
    deleted_broadcast_recipients: int = 0
    deleted_support_messages: int = 0
    deleted_support_cases: int = 0
    deleted_feedback_items: int = 0
    deleted_poll_recipients: int = 0
    deleted_poll_responses: int = 0
    deleted_ai_transaction_drafts: int = 0
    deleted_saving_prompt_settings: int = 0
    deleted_pending_saving_tasks: int = 0
    deleted_debt_invites: int = 0
    deleted_family_members: int = 0
    deleted_family_invites: int = 0
    deleted_families: int = 0
    deleted_billing_profiles: int = 0
    deleted_payments: int = 0
    deleted_subscriptions: int = 0
    deleted_subscription_events: int = 0
    deleted_promo_claims: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "deactivated_accounts": self.deactivated_accounts,
            "deactivated_categories": self.deactivated_categories,
            "deleted_accounts": self.deleted_accounts,
            "deleted_account_admin_states": self.deleted_account_admin_states,
            "deleted_categories": self.deleted_categories,
            "deleted_transactions": self.deleted_transactions,
            "deleted_debt_payments": self.deleted_debt_payments,
            "deleted_debts": self.deleted_debts,
            "deleted_bot_events": self.deleted_bot_events,
            "deleted_admin_message_logs": self.deleted_admin_message_logs,
            "deleted_broadcast_recipients": self.deleted_broadcast_recipients,
            "deleted_support_messages": self.deleted_support_messages,
            "deleted_support_cases": self.deleted_support_cases,
            "deleted_feedback_items": self.deleted_feedback_items,
            "deleted_poll_recipients": self.deleted_poll_recipients,
            "deleted_poll_responses": self.deleted_poll_responses,
            "deleted_ai_transaction_drafts": self.deleted_ai_transaction_drafts,
            "deleted_saving_prompt_settings": self.deleted_saving_prompt_settings,
            "deleted_pending_saving_tasks": self.deleted_pending_saving_tasks,
            "deleted_debt_invites": self.deleted_debt_invites,
            "deleted_family_members": self.deleted_family_members,
            "deleted_family_invites": self.deleted_family_invites,
            "deleted_families": self.deleted_families,
            "deleted_billing_profiles": self.deleted_billing_profiles,
            "deleted_payments": self.deleted_payments,
            "deleted_subscriptions": self.deleted_subscriptions,
            "deleted_subscription_events": self.deleted_subscription_events,
            "deleted_promo_claims": self.deleted_promo_claims,
        }


@dataclass(slots=True)
class HardDeleteStats:
    deleted_user_rows: int = 0
    deleted_auth_sessions: int = 0
    deleted_auth_identities: int = 0
    deleted_oidc_token_uses: int = 0
    deleted_browser_login_token_uses: int = 0
    anonymized_oidc_token_uses: int = 0
    anonymized_browser_login_token_uses: int = 0
    deleted_miniapp_write_receipts: int = 0
    deleted_miniapp_draft_actions: int = 0
    deleted_install_nudge_states: int = 0
    deleted_push_deliveries: int = 0
    deleted_web_push_subscriptions: int = 0
    deleted_notification_preferences: int = 0
    deleted_app_notifications: int = 0
    deleted_daily_expense_reminder_settings: int = 0
    deleted_funnel_events: int = 0
    deleted_acquisition_sessions: int = 0
    deleted_billing_consents: int = 0
    deleted_admin_states: int = 0
    deleted_admin_notes: int = 0
    deleted_user_tags: int = 0
    deleted_user_push_topics: int = 0
    deleted_bot_events: int = 0
    deleted_admin_message_logs: int = 0
    deleted_broadcast_recipients: int = 0
    deleted_support_messages: int = 0
    deleted_support_cases: int = 0
    deleted_feedback_items: int = 0
    deleted_poll_recipients: int = 0
    deleted_poll_responses: int = 0
    deleted_account_admin_states: int = 0
    deleted_accounts: int = 0
    deleted_categories: int = 0
    deleted_transactions: int = 0
    deleted_debt_payments: int = 0
    deleted_debts: int = 0
    deleted_billing_profiles: int = 0
    deleted_payments: int = 0
    deleted_subscriptions: int = 0
    deleted_subscription_events: int = 0
    deleted_promo_claims: int = 0
    deleted_ai_transaction_drafts: int = 0
    deleted_saving_prompt_settings: int = 0
    deleted_pending_saving_tasks: int = 0
    deleted_debt_invites: int = 0
    deleted_family_members: int = 0
    deleted_family_invites: int = 0
    deleted_families: int = 0
    nullified_family_member_invited_by: int = 0
    nullified_family_invite_used_by: int = 0
    nullified_account_created_by: int = 0
    nullified_category_created_by: int = 0
    nullified_transaction_created_by: int = 0
    nullified_transaction_updated_by: int = 0
    nullified_transaction_deleted_by: int = 0
    nullified_debt_borrowers: int = 0
    deleted_audit_logs: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "deleted_user_rows": self.deleted_user_rows,
            "deleted_auth_sessions": self.deleted_auth_sessions,
            "deleted_auth_identities": self.deleted_auth_identities,
            "deleted_oidc_token_uses": self.deleted_oidc_token_uses,
            "deleted_browser_login_token_uses": self.deleted_browser_login_token_uses,
            "anonymized_oidc_token_uses": self.anonymized_oidc_token_uses,
            "anonymized_browser_login_token_uses": self.anonymized_browser_login_token_uses,
            "deleted_miniapp_write_receipts": self.deleted_miniapp_write_receipts,
            "deleted_miniapp_draft_actions": self.deleted_miniapp_draft_actions,
            "deleted_install_nudge_states": self.deleted_install_nudge_states,
            "deleted_push_deliveries": self.deleted_push_deliveries,
            "deleted_web_push_subscriptions": self.deleted_web_push_subscriptions,
            "deleted_notification_preferences": self.deleted_notification_preferences,
            "deleted_app_notifications": self.deleted_app_notifications,
            "deleted_daily_expense_reminder_settings": self.deleted_daily_expense_reminder_settings,
            "deleted_funnel_events": self.deleted_funnel_events,
            "deleted_acquisition_sessions": self.deleted_acquisition_sessions,
            "deleted_billing_consents": self.deleted_billing_consents,
            "deleted_admin_states": self.deleted_admin_states,
            "deleted_admin_notes": self.deleted_admin_notes,
            "deleted_user_tags": self.deleted_user_tags,
            "deleted_user_push_topics": self.deleted_user_push_topics,
            "deleted_bot_events": self.deleted_bot_events,
            "deleted_admin_message_logs": self.deleted_admin_message_logs,
            "deleted_broadcast_recipients": self.deleted_broadcast_recipients,
            "deleted_support_messages": self.deleted_support_messages,
            "deleted_support_cases": self.deleted_support_cases,
            "deleted_feedback_items": self.deleted_feedback_items,
            "deleted_poll_recipients": self.deleted_poll_recipients,
            "deleted_poll_responses": self.deleted_poll_responses,
            "deleted_account_admin_states": self.deleted_account_admin_states,
            "deleted_accounts": self.deleted_accounts,
            "deleted_categories": self.deleted_categories,
            "deleted_transactions": self.deleted_transactions,
            "deleted_debt_payments": self.deleted_debt_payments,
            "deleted_debts": self.deleted_debts,
            "deleted_billing_profiles": self.deleted_billing_profiles,
            "deleted_payments": self.deleted_payments,
            "deleted_subscriptions": self.deleted_subscriptions,
            "deleted_subscription_events": self.deleted_subscription_events,
            "deleted_promo_claims": self.deleted_promo_claims,
            "deleted_ai_transaction_drafts": self.deleted_ai_transaction_drafts,
            "deleted_saving_prompt_settings": self.deleted_saving_prompt_settings,
            "deleted_pending_saving_tasks": self.deleted_pending_saving_tasks,
            "deleted_debt_invites": self.deleted_debt_invites,
            "deleted_family_members": self.deleted_family_members,
            "deleted_family_invites": self.deleted_family_invites,
            "deleted_families": self.deleted_families,
            "nullified_family_member_invited_by": self.nullified_family_member_invited_by,
            "nullified_family_invite_used_by": self.nullified_family_invite_used_by,
            "nullified_account_created_by": self.nullified_account_created_by,
            "nullified_category_created_by": self.nullified_category_created_by,
            "nullified_transaction_created_by": self.nullified_transaction_created_by,
            "nullified_transaction_updated_by": self.nullified_transaction_updated_by,
            "nullified_transaction_deleted_by": self.nullified_transaction_deleted_by,
            "nullified_debt_borrowers": self.nullified_debt_borrowers,
            "deleted_audit_logs": self.deleted_audit_logs,
        }


def allowlisted_test_telegram_ids() -> set[int]:
    return set(getattr(settings, "ADMIN_TEST_TELEGRAM_IDS", []))


def is_allowlisted_test_user(user: TelegramUser) -> bool:
    return user.tg_user_id in allowlisted_test_telegram_ids()


def get_or_create_admin_state(user: TelegramUser) -> UserAdminState:
    state, _ = UserAdminState.objects.get_or_create(telegram_user_id=user.tg_user_id)
    return state


def get_bot_setting_value(key: str, default=""):
    setting = BotSetting.objects.filter(key=key).first()
    if setting is None:
        return default
    value = setting.parsed_value()
    if value in {None, ""}:
        return default
    return value


def get_or_create_user_tag(
    *,
    name: str,
    admin_user,
    request=None,
) -> tuple[Tag, bool]:
    require_service_action(admin_user, "tag_create", request=request)
    normalized_name = " ".join((name or "").split()).strip()
    if not normalized_name:
        raise ValueError("Tag name is required.")

    existing = Tag.objects.filter(name__iexact=normalized_name).first()
    if existing is not None:
        return existing, False

    tag = Tag.objects.create(name=normalized_name)
    create_audit_log(
        request=request,
        admin_user=admin_user,
        action="user_tag_created",
        object_type="tag",
        object_id=tag.pk,
        after={"tag_id": tag.pk, "name": tag.name, "slug": tag.slug},
    )
    return tag, True


def _test_mark_snapshot(state: UserAdminState) -> dict:
    # Explicit action allowlist: never copy raw onboarding/finance input to audit.
    return {
        "is_test_user": state.is_test_user,
        "marked_as_test_by_id": state.marked_as_test_by_id,
        "marked_as_test_at": state.marked_as_test_at.isoformat() if state.marked_as_test_at else None,
    }


def mark_user_as_test_user(
    *,
    user: TelegramUser,
    admin_user,
    notes: str = "",
    request=None,
) -> UserAdminState:
    require_service_action(admin_user, "test_mark", request=request)
    state = get_or_create_admin_state(user)
    before = _test_mark_snapshot(state)
    state.is_test_user = True
    if notes:
        state.test_user_notes = notes
    state.marked_as_test_by = admin_user
    state.marked_as_test_at = timezone.now()
    state.save(
        update_fields=[
            "is_test_user",
            "test_user_notes",
            "marked_as_test_by",
            "marked_as_test_at",
            "updated_at",
        ]
    )
    create_audit_log(
        request=request,
        admin_user=admin_user,
        action="test_user_marked",
        object_type="telegram_user",
        object_id=user.pk,
        target_user_id=user.tg_user_id,
        before=before,
        after=_test_mark_snapshot(state),
    )
    return state


def unmark_user_as_test_user(
    *,
    user: TelegramUser,
    admin_user,
    request=None,
) -> UserAdminState:
    require_service_action(admin_user, "test_mark", request=request)
    state = get_or_create_admin_state(user)
    before = _test_mark_snapshot(state)
    state.is_test_user = False
    state.marked_as_test_by = None
    state.marked_as_test_at = None
    state.save(
        update_fields=[
            "is_test_user",
            "marked_as_test_by",
            "marked_as_test_at",
            "updated_at",
        ]
    )
    create_audit_log(
        request=request,
        admin_user=admin_user,
        action="test_user_unmarked",
        object_type="telegram_user",
        object_id=user.pk,
        target_user_id=user.tg_user_id,
        before=before,
        after=_test_mark_snapshot(state),
    )
    return state


def _latest_subscription(user: TelegramUser) -> Subscription | None:
    return Subscription.objects.filter(user_id=user.tg_user_id).order_by("-created_at").first()


def assign_tag_to_users(
    *,
    tag: Tag,
    users: list[TelegramUser] | tuple[TelegramUser, ...],
    admin_user,
    request=None,
) -> int:
    require_service_action(admin_user, "tag_assign", request=request)
    user_ids = [user.tg_user_id for user in users]
    if not user_ids:
        return 0

    existing_ids = set(UserTag.objects.filter(user_id__in=user_ids, tag=tag).values_list("user_id", flat=True))
    rows = [
        UserTag(user_id=user_id, tag=tag, created_by=admin_user)
        for user_id in user_ids
        if user_id not in existing_ids
    ]
    if rows:
        UserTag.objects.bulk_create(rows, ignore_conflicts=True)
    create_audit_log(
        request=request,
        admin_user=admin_user,
        action="user_tag_bulk_assigned",
        object_type="tag",
        object_id=tag.pk,
        after={
            "tag_id": tag.pk,
            "user_ids": user_ids[:100],
            "user_count": len(user_ids),
            "created_count": len(rows),
        },
    )
    return len(rows)


def remove_tag_from_users(
    *,
    tag: Tag,
    users: list[TelegramUser] | tuple[TelegramUser, ...],
    admin_user,
    request=None,
) -> int:
    require_service_action(admin_user, "tag_remove", request=request)
    user_ids = [user.tg_user_id for user in users]
    if not user_ids:
        return 0

    removed_count, _ = UserTag.objects.filter(user_id__in=user_ids, tag=tag).delete()
    create_audit_log(
        request=request,
        admin_user=admin_user,
        action="user_tag_bulk_removed",
        object_type="tag",
        object_id=tag.pk,
        after={
            "tag_id": tag.pk,
            "user_ids": user_ids[:100],
            "user_count": len(user_ids),
            "removed_count": removed_count,
        },
    )
    return removed_count


def _snapshot_user(user: TelegramUser, state: UserAdminState) -> dict:
    """Minimal reset receipt, safe for both audit storage and operator JSON."""
    return {
        "user": {
            "tg_user_id": user.tg_user_id,
            "onboarding_completed": user.onboarding_completed,
            "onboarding_version": user.onboarding_version,
        },
        "admin_state": {
            "status": state.status,
            "subscription_status": state.subscription_status,
            "is_test_user": state.is_test_user,
            "pending_admin_reset_mode": state.pending_admin_reset_mode,
        },
    }


def _validate_reset_mode(*, user: TelegramUser, state: UserAdminState, admin_user, mode: str, options: dict) -> None:
    require_service_action(admin_user, "onboarding_reset" if mode == "safe" else "test_cleanup", request=options.get("request"))
    if mode not in {"safe", "cleanup", "full", "replay"}:
        raise ResetOnboardingError(f"Unsupported reset mode: {mode}")
    if not options.get("reason"):
        raise ResetOnboardingError("Reset reason is required.")

    is_test_target = state.is_test_user or is_allowlisted_test_user(user)
    latest_subscription = _latest_subscription(user)
    has_paid_access = bool(
        latest_subscription
        and latest_subscription.status
        in {
            Subscription.Status.ACTIVE,
            Subscription.Status.PAID,
            Subscription.Status.MANUAL,
            Subscription.Status.LIFETIME,
        }
    )

    if mode in {"cleanup", "full", "replay"} and not is_test_target:
        raise ResetOnboardingError("Режими очищення тестових даних доступні лише для тестових користувачів.")
    if mode in {"cleanup", "full", "replay"} and not options.get("double_confirmed"):
        raise ResetOnboardingError("Для очищення тестових даних потрібне подвійне підтвердження.")
    is_privileged_operator = admin_user is None and authenticated_operator_id(options.get("request")) is not None
    is_superuser = bool(getattr(admin_user, "is_superuser", False))
    if mode == "replay" and not (is_superuser or is_privileged_operator):
        raise ResetOnboardingError("Стирання даних користувача доступне лише для superuser.")
    if mode == "replay" and str(options.get("confirmation_text") or "").strip() != REPLAY_RESET_CONFIRMATION:
        raise ResetOnboardingError(f'Для стирання даних введіть точне підтвердження "{REPLAY_RESET_CONFIRMATION}".')
    if mode == "full" and not (is_superuser or is_privileged_operator):
        raise ResetOnboardingError("Повне скидання доступне лише для superuser.")
    if mode == "full" and str(options.get("confirmation_text") or "").strip() != "RESET USER":
        raise ResetOnboardingError('Для повного скидання введіть підтвердження "RESET USER".')
    if mode in {"cleanup", "full"} and has_paid_access and not state.is_test_user:
        raise ResetOnboardingError("Очищення тестових даних заборонене для платних користувачів без позначки тестового.")


def _cleanup_user_finance_data(user: TelegramUser) -> CleanupStats:
    now = timezone.now()
    stats = CleanupStats()
    stats.deleted_transactions, _ = Transaction.all_objects.filter(tg_user_id=user.tg_user_id).delete()
    stats.deleted_debt_payments, _ = DebtPayment.objects.filter(tg_user_id=user.tg_user_id).delete()
    stats.deleted_debts, _ = Debt.objects.filter(tg_user_id=user.tg_user_id).delete()
    stats.deactivated_categories = Category.objects.filter(
        user_id=user.tg_user_id,
        is_system=False,
        is_active=True,
    ).update(is_active=False, deleted_at=now)
    stats.deactivated_accounts = Account.objects.filter(
        tg_user_id=user.tg_user_id,
        is_active=True,
    ).update(is_active=False, updated_at=now)
    AccountAdminState.objects.filter(account__tg_user_id=user.tg_user_id).update(
        is_default=False,
        is_active=False,
        updated_at=now,
    )
    return stats


def _cleanup_user_billing_data(user: TelegramUser, stats: CleanupStats) -> CleanupStats:
    stats.deleted_promo_claims, _ = PromoOfferClaim.objects.filter(user_id=user.tg_user_id).delete()
    stats.deleted_payments, _ = Payment.objects.filter(user_id=user.tg_user_id).delete()
    stats.deleted_subscription_events, _ = SubscriptionEvent.objects.filter(user_id=user.tg_user_id).delete()
    stats.deleted_subscriptions, _ = Subscription.objects.filter(user_id=user.tg_user_id).delete()
    stats.deleted_billing_profiles, _ = BillingProfile.objects.filter(user_id=user.tg_user_id).delete()
    return stats


def _cleanup_user_replay_data(user: TelegramUser) -> CleanupStats:
    stats = CleanupStats()
    existing_tables = _existing_table_names()
    columns_cache: dict[str, set[str]] = {}

    stats.deleted_bot_events = _delete_rows(
        table_name="bot_events",
        where_sql="telegram_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    stats.deleted_admin_message_logs = _delete_rows(
        table_name="admin_message_log",
        where_sql="telegram_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    stats.deleted_broadcast_recipients = _delete_rows(
        table_name="broadcast_recipients",
        where_sql="telegram_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    if "feedback_items" in existing_tables and "support_cases" in existing_tables:
        stats.deleted_feedback_items = _delete_rows(
            table_name="feedback_items",
            where_sql=(
                "telegram_user_id = %s "
                "OR support_case_id IN (SELECT id FROM support_cases WHERE telegram_user_id = %s)"
            ),
            params=[user.tg_user_id, user.tg_user_id],
            existing_tables=existing_tables,
        )
    else:
        stats.deleted_feedback_items = _delete_rows(
            table_name="feedback_items",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
    if "support_messages" in existing_tables and "support_cases" in existing_tables:
        stats.deleted_support_messages = _delete_rows(
            table_name="support_messages",
            where_sql="case_id IN (SELECT id FROM support_cases WHERE telegram_user_id = %s)",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
    stats.deleted_support_cases = _delete_rows(
        table_name="support_cases",
        where_sql="telegram_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    stats.deleted_poll_responses = _delete_rows(
        table_name="poll_responses",
        where_sql="telegram_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    stats.deleted_poll_recipients = _delete_rows(
        table_name="poll_recipients",
        where_sql="telegram_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    stats.deleted_ai_transaction_drafts = _delete_rows(
        table_name="ai_transaction_drafts",
        where_sql="tg_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    stats.deleted_saving_prompt_settings = _delete_rows(
        table_name="saving_prompt_settings",
        where_sql="tg_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    stats.deleted_pending_saving_tasks = _delete_rows(
        table_name="pending_saving_tasks",
        where_sql="tg_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    stats.deleted_transactions = _delete_rows(
        table_name="transactions",
        where_sql="tg_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    stats.deleted_debt_payments = _delete_rows(
        table_name="debt_payments",
        where_sql="tg_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    stats.deleted_debts = _delete_rows(
        table_name="debts",
        where_sql="tg_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    if (
        "accounts" in existing_tables
        and _table_has_columns("account_admin_states", "account_id", existing_tables=existing_tables, cache=columns_cache)
    ):
        stats.deleted_account_admin_states = _delete_rows(
            table_name="account_admin_states",
            where_sql="account_id IN (SELECT id FROM accounts WHERE tg_user_id = %s)",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
    stats.deleted_accounts = _delete_rows(
        table_name="accounts",
        where_sql="tg_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    if _table_has_columns("categories", "user_id", "tg_user_id", existing_tables=existing_tables, cache=columns_cache):
        stats.deleted_categories = _delete_rows(
            table_name="categories",
            where_sql="COALESCE(user_id, tg_user_id) = %s OR tg_user_id = %s",
            params=[user.tg_user_id, user.tg_user_id],
            existing_tables=existing_tables,
        )
    stats.deleted_debt_invites = _delete_rows(
        table_name="debt_invites",
        where_sql="created_by_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    if _table_has_columns("family_invites", "used_by_user_id", "created_by_user_id", existing_tables=existing_tables, cache=columns_cache):
        _update_rows(
            table_name="family_invites",
            set_sql="used_by_user_id = NULL",
            where_sql="used_by_user_id = %s AND created_by_user_id <> %s",
            params=[user.tg_user_id, user.tg_user_id],
            existing_tables=existing_tables,
            cache=columns_cache,
            required_columns=("used_by_user_id", "created_by_user_id"),
        )
    owned_family_ids = _select_ids(
        table_name="families",
        column_name="id",
        where_sql="owner_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    if owned_family_ids:
        placeholders, placeholder_params = _sql_placeholders(owned_family_ids)
        if _table_has_columns("accounts", "family_id", "tg_user_id", existing_tables=existing_tables, cache=columns_cache):
            _update_rows(
                table_name="accounts",
                set_sql="family_id = NULL",
                where_sql=f"family_id IN ({placeholders}) AND tg_user_id <> %s",
                params=[*placeholder_params, user.tg_user_id],
                existing_tables=existing_tables,
                cache=columns_cache,
                required_columns=("family_id", "tg_user_id"),
            )
        if _table_has_columns(
            "categories",
            "family_id",
            "user_id",
            "tg_user_id",
            existing_tables=existing_tables,
            cache=columns_cache,
        ):
            _update_rows(
                table_name="categories",
                set_sql="family_id = NULL",
                where_sql=f"family_id IN ({placeholders}) AND COALESCE(user_id, tg_user_id) <> %s",
                params=[*placeholder_params, user.tg_user_id],
                existing_tables=existing_tables,
                cache=columns_cache,
                required_columns=("family_id", "user_id", "tg_user_id"),
            )
        if _table_has_columns("transactions", "family_id", "tg_user_id", existing_tables=existing_tables, cache=columns_cache):
            _update_rows(
                table_name="transactions",
                set_sql="family_id = NULL",
                where_sql=f"family_id IN ({placeholders}) AND tg_user_id <> %s",
                params=[*placeholder_params, user.tg_user_id],
                existing_tables=existing_tables,
                cache=columns_cache,
                required_columns=("family_id", "tg_user_id"),
            )
        if _table_has_columns("debts", "family_id", "tg_user_id", existing_tables=existing_tables, cache=columns_cache):
            _update_rows(
                table_name="debts",
                set_sql="family_id = NULL",
                where_sql=f"family_id IN ({placeholders}) AND tg_user_id <> %s",
                params=[*placeholder_params, user.tg_user_id],
                existing_tables=existing_tables,
                cache=columns_cache,
                required_columns=("family_id", "tg_user_id"),
            )
        if _table_has_columns("debt_payments", "family_id", "tg_user_id", existing_tables=existing_tables, cache=columns_cache):
            _update_rows(
                table_name="debt_payments",
                set_sql="family_id = NULL",
                where_sql=f"family_id IN ({placeholders}) AND tg_user_id <> %s",
                params=[*placeholder_params, user.tg_user_id],
                existing_tables=existing_tables,
                cache=columns_cache,
                required_columns=("family_id", "tg_user_id"),
            )
        stats.deleted_family_invites += _delete_rows(
            table_name="family_invites",
            where_sql=f"family_id IN ({placeholders})",
            params=placeholder_params,
            existing_tables=existing_tables,
        )
        stats.deleted_family_members += _delete_rows(
            table_name="family_members",
            where_sql=f"family_id IN ({placeholders})",
            params=placeholder_params,
            existing_tables=existing_tables,
        )
    stats.deleted_family_invites += _delete_rows(
        table_name="family_invites",
        where_sql="created_by_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    stats.deleted_family_members += _delete_rows(
        table_name="family_members",
        where_sql="user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    stats.deleted_families = _delete_rows(
        table_name="families",
        where_sql="owner_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    return stats


def _queue_reset_notice(*, user: TelegramUser, admin_user, text: str) -> None:
    message_log = AdminMessageLog.objects.create(
        telegram_user_id=user.tg_user_id,
        admin_user=admin_user,
        message_text=text,
        parse_mode="none",
        send_test_to_admin_first=False,
        target_chat_id=user.tg_user_id,
    )
    send_manual_message_task.delay(message_log.id, is_test=False)


def _queue_admin_notice(*, user: TelegramUser, mode: str, reason: str, cleanup_stats: dict[str, int]) -> None:
    username_text = f"@{user.username}" if user.username else "-"
    text = (
        "Onboarding reset completed\n\n"
        f"telegram_id: {user.tg_user_id}\n"
        f"username: {username_text}\n"
    )
    text += (
        f"mode: {mode}\n"
        f"reason: {reason[:200]}\n"
        f"cleanup: {cleanup_stats}\n"
        f"time: {timezone.localtime().strftime('%Y-%m-%d %H:%M')}"
    )
    send_admin_notification_task.delay(
        "onboarding_reset_completed",
        text,
        {"telegram_user_id": user.tg_user_id, "mode": mode, "cleanup": cleanup_stats},
    )


def _existing_table_names() -> set[str]:
    return set(connection.introspection.table_names())


def _table_columns(table_name: str, *, existing_tables: set[str], cache: dict[str, set[str]]) -> set[str]:
    if table_name not in existing_tables:
        return set()
    if table_name not in cache:
        with connection.cursor() as cursor:
            cache[table_name] = {
                column.name
                for column in connection.introspection.get_table_description(cursor, table_name)
            }
    return cache[table_name]


def _table_has_columns(table_name: str, *columns: str, existing_tables: set[str], cache: dict[str, set[str]]) -> bool:
    return set(columns).issubset(_table_columns(table_name, existing_tables=existing_tables, cache=cache))


def _delete_rows(*, table_name: str, where_sql: str, params: list, existing_tables: set[str]) -> int:
    if table_name not in existing_tables:
        return 0
    quoted_table = connection.ops.quote_name(table_name)
    with connection.cursor() as cursor:
        cursor.execute(f"DELETE FROM {quoted_table} WHERE {where_sql}", params)
        return cursor.rowcount or 0


def _delete_privacy_identity_rows(
    *,
    user_id: int,
    stats: HardDeleteStats,
    existing_tables: set[str],
) -> None:
    """Delete user-linked auth, PWA and acquisition rows in FK-safe order."""

    params = [user_id]
    stats.deleted_billing_consents = _delete_rows(
        table_name="billing_consents",
        where_sql="telegram_user_id = %s",
        params=params,
        existing_tables=existing_tables,
    )
    stats.deleted_funnel_events = _delete_rows(
        table_name="funnel_events",
        where_sql=(
            "acquisition_session_id IN "
            "(SELECT id FROM acquisition_sessions WHERE telegram_user_id = %s)"
        ),
        params=params,
        existing_tables=(existing_tables if "acquisition_sessions" in existing_tables else set()),
    )
    stats.deleted_acquisition_sessions = _delete_rows(
        table_name="acquisition_sessions",
        where_sql="telegram_user_id = %s",
        params=params,
        existing_tables=existing_tables,
    )

    stats.deleted_auth_sessions = _delete_rows(
        table_name="user_auth_sessions",
        where_sql="telegram_user_id = %s",
        params=params,
        existing_tables=existing_tables,
    )
    stats.deleted_auth_identities = _delete_rows(
        table_name="user_auth_identities",
        where_sql="telegram_user_id = %s",
        params=params,
        existing_tables=existing_tables,
    )
    stats.deleted_oidc_token_uses = _delete_rows(
        table_name="user_oidc_token_uses",
        where_sql="tg_user_id = %s",
        params=params,
        existing_tables=existing_tables,
    )
    stats.deleted_browser_login_token_uses = _delete_rows(
        table_name="miniapp_browser_login_token_uses",
        where_sql="tg_user_id = %s",
        params=params,
        existing_tables=existing_tables,
    )
    stats.deleted_miniapp_write_receipts = _delete_rows(
        table_name="miniapp_write_receipts",
        where_sql="tg_user_id = %s",
        params=params,
        existing_tables=existing_tables,
    )
    stats.deleted_miniapp_draft_actions = _delete_rows(
        table_name="miniapp_draft_actions",
        where_sql="tg_user_id = %s",
        params=params,
        existing_tables=existing_tables,
    )
    stats.deleted_install_nudge_states = _delete_rows(
        table_name="miniapp_install_nudge_states",
        where_sql="tg_user_id = %s",
        params=params,
        existing_tables=existing_tables,
    )

    push_tables = {
        "miniapp_push_deliveries",
        "miniapp_app_notifications",
        "miniapp_web_push_subscriptions",
    }
    stats.deleted_push_deliveries = _delete_rows(
        table_name="miniapp_push_deliveries",
        where_sql=(
            "notification_id IN (SELECT id FROM miniapp_app_notifications WHERE tg_user_id = %s) "
            "OR subscription_id IN (SELECT id FROM miniapp_web_push_subscriptions WHERE tg_user_id = %s)"
        ),
        params=[user_id, user_id],
        existing_tables=(existing_tables if push_tables.issubset(existing_tables) else set()),
    )
    stats.deleted_app_notifications = _delete_rows(
        table_name="miniapp_app_notifications",
        where_sql="tg_user_id = %s",
        params=params,
        existing_tables=existing_tables,
    )
    stats.deleted_web_push_subscriptions = _delete_rows(
        table_name="miniapp_web_push_subscriptions",
        where_sql="tg_user_id = %s",
        params=params,
        existing_tables=existing_tables,
    )
    stats.deleted_notification_preferences = _delete_rows(
        table_name="miniapp_notification_preferences",
        where_sql="tg_user_id = %s",
        params=params,
        existing_tables=existing_tables,
    )
    stats.deleted_daily_expense_reminder_settings = _delete_rows(
        table_name="daily_expense_reminder_settings",
        where_sql="tg_user_id = %s",
        params=params,
        existing_tables=existing_tables,
    )


def _update_rows(
    *,
    table_name: str,
    set_sql: str,
    where_sql: str,
    params: list,
    existing_tables: set[str],
    cache: dict[str, set[str]],
    required_columns: tuple[str, ...] = (),
) -> int:
    if table_name not in existing_tables:
        return 0
    if required_columns and not _table_has_columns(table_name, *required_columns, existing_tables=existing_tables, cache=cache):
        return 0
    quoted_table = connection.ops.quote_name(table_name)
    with connection.cursor() as cursor:
        cursor.execute(f"UPDATE {quoted_table} SET {set_sql} WHERE {where_sql}", params)
        return cursor.rowcount or 0


def _select_count(*, table_name: str, where_sql: str, params: list, existing_tables: set[str]) -> int:
    if table_name not in existing_tables:
        return 0
    quoted_table = connection.ops.quote_name(table_name)
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {quoted_table} WHERE {where_sql}", params)
        row = cursor.fetchone()
    return int(row[0] or 0) if row else 0


def _select_ids(*, table_name: str, column_name: str, where_sql: str, params: list, existing_tables: set[str]) -> list[int]:
    if table_name not in existing_tables:
        return []
    quoted_table = connection.ops.quote_name(table_name)
    quoted_column = connection.ops.quote_name(column_name)
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT {quoted_column} FROM {quoted_table} WHERE {where_sql}", params)
        rows = cursor.fetchall()
    return [int(row[0]) for row in rows]


def _sql_placeholders(values: list[int]) -> tuple[str, list[int]]:
    return ", ".join(["%s"] * len(values)), list(values)


def _lock_owned_family_ids(*, user_id: int, existing_tables: set[str]) -> list[int]:
    if "families" not in existing_tables:
        return []
    quoted_table = connection.ops.quote_name("families")
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT id FROM {quoted_table} WHERE owner_user_id = %s ORDER BY id FOR UPDATE",
            [user_id],
        )
        return [int(row[0]) for row in cursor.fetchall()]


def _validate_hard_delete(*, user: TelegramUser, state: UserAdminState | None, admin_user, options: dict) -> None:
    require_service_action(admin_user, "test_cleanup", request=options.get("request"))
    if not options.get("reason"):
        raise HardDeleteUserError("Причина видалення обов'язкова.")
    if not (getattr(admin_user, "is_superuser", False) or (admin_user is None and authenticated_operator_id(options.get("request")) is not None)):
        raise HardDeleteUserError("Hard delete доступний лише для superuser.")

    is_test_target = bool(state and state.is_test_user) or is_allowlisted_test_user(user)
    if not is_test_target:
        raise HardDeleteUserError("Hard delete доступний лише для тестових користувачів.")
    if not options.get("double_confirmed"):
        raise HardDeleteUserError("Потрібне подвійне підтвердження безповоротного видалення.")

    expected = f"DELETE USER {user.tg_user_id}"
    provided = str(options.get("confirmation_text") or "").strip()
    if provided != expected:
        raise HardDeleteUserError(f'Для hard delete введіть точне підтвердження "{expected}".')


def _validate_family_delete_safety(
    *,
    user: TelegramUser,
    owned_family_ids: list[int],
    existing_tables: set[str],
    cache: dict[str, set[str]],
) -> None:
    if not owned_family_ids:
        return

    placeholders, placeholder_params = _sql_placeholders(owned_family_ids)

    other_members = _select_count(
        table_name="family_members",
        where_sql=f"family_id IN ({placeholders}) AND user_id <> %s",
        params=[*placeholder_params, user.tg_user_id],
        existing_tables=existing_tables,
    )
    if other_members:
        raise HardDeleteUserError("Користувач володіє сім'єю з іншими учасниками. Спершу приберіть інших учасників із сім'ї.")

    family_blockers: list[str] = []
    if _table_has_columns("accounts", "family_id", "tg_user_id", existing_tables=existing_tables, cache=cache):
        blocker_count = _select_count(
            table_name="accounts",
            where_sql=f"family_id IN ({placeholders}) AND tg_user_id <> %s",
            params=[*placeholder_params, user.tg_user_id],
            existing_tables=existing_tables,
        )
        if blocker_count:
            family_blockers.append(f"accounts={blocker_count}")

    if _table_has_columns("categories", "family_id", "user_id", "tg_user_id", existing_tables=existing_tables, cache=cache):
        blocker_count = _select_count(
            table_name="categories",
            where_sql=f"family_id IN ({placeholders}) AND COALESCE(user_id, tg_user_id) <> %s",
            params=[*placeholder_params, user.tg_user_id],
            existing_tables=existing_tables,
        )
        if blocker_count:
            family_blockers.append(f"categories={blocker_count}")

    if _table_has_columns("debts", "family_id", "tg_user_id", existing_tables=existing_tables, cache=cache):
        blocker_count = _select_count(
            table_name="debts",
            where_sql=f"family_id IN ({placeholders}) AND tg_user_id <> %s",
            params=[*placeholder_params, user.tg_user_id],
            existing_tables=existing_tables,
        )
        if blocker_count:
            family_blockers.append(f"debts={blocker_count}")

    if _table_has_columns("debt_payments", "family_id", "tg_user_id", existing_tables=existing_tables, cache=cache):
        blocker_count = _select_count(
            table_name="debt_payments",
            where_sql=f"family_id IN ({placeholders}) AND tg_user_id <> %s",
            params=[*placeholder_params, user.tg_user_id],
            existing_tables=existing_tables,
        )
        if blocker_count:
            family_blockers.append(f"debt_payments={blocker_count}")

    if family_blockers:
        raise HardDeleteUserError(
            "Не можна hard-delete тестового користувача, бо його сім'я містить чужі дані: "
            + ", ".join(family_blockers)
            + "."
        )


def _validate_replay_family_safety(*, user: TelegramUser) -> None:
    existing_tables = _existing_table_names()
    columns_cache: dict[str, set[str]] = {}
    owned_family_ids = _select_ids(
        table_name="families",
        column_name="id",
        where_sql="owner_user_id = %s",
        params=[user.tg_user_id],
        existing_tables=existing_tables,
    )
    if not owned_family_ids:
        return

    placeholders, placeholder_params = _sql_placeholders(owned_family_ids)

    other_members = _select_count(
        table_name="family_members",
        where_sql=f"family_id IN ({placeholders}) AND user_id <> %s",
        params=[*placeholder_params, user.tg_user_id],
        existing_tables=existing_tables,
    )
    if other_members:
        raise ResetOnboardingError("Не можна стерти дані, поки у family space є інші учасники.")

    family_blockers: list[str] = []
    if _table_has_columns("accounts", "family_id", "tg_user_id", existing_tables=existing_tables, cache=columns_cache):
        blocker_count = _select_count(
            table_name="accounts",
            where_sql=f"family_id IN ({placeholders}) AND tg_user_id <> %s",
            params=[*placeholder_params, user.tg_user_id],
            existing_tables=existing_tables,
        )
        if blocker_count:
            family_blockers.append(f"accounts={blocker_count}")

    if _table_has_columns("categories", "family_id", "user_id", "tg_user_id", existing_tables=existing_tables, cache=columns_cache):
        blocker_count = _select_count(
            table_name="categories",
            where_sql=f"family_id IN ({placeholders}) AND COALESCE(user_id, tg_user_id) <> %s",
            params=[*placeholder_params, user.tg_user_id],
            existing_tables=existing_tables,
        )
        if blocker_count:
            family_blockers.append(f"categories={blocker_count}")

    if _table_has_columns("debts", "family_id", "tg_user_id", existing_tables=existing_tables, cache=columns_cache):
        blocker_count = _select_count(
            table_name="debts",
            where_sql=f"family_id IN ({placeholders}) AND tg_user_id <> %s",
            params=[*placeholder_params, user.tg_user_id],
            existing_tables=existing_tables,
        )
        if blocker_count:
            family_blockers.append(f"debts={blocker_count}")

    if _table_has_columns("debt_payments", "family_id", "tg_user_id", existing_tables=existing_tables, cache=columns_cache):
        blocker_count = _select_count(
            table_name="debt_payments",
            where_sql=f"family_id IN ({placeholders}) AND tg_user_id <> %s",
            params=[*placeholder_params, user.tg_user_id],
            existing_tables=existing_tables,
        )
        if blocker_count:
            family_blockers.append(f"debt_payments={blocker_count}")

    if family_blockers:
        raise ResetOnboardingError(
            "Не можна стерти дані, бо family space містить чужі дані: " + ", ".join(family_blockers) + "."
        )


def reset_user_onboarding(
    user_id: int,
    mode: str,
    admin_user,
    reason: str,
    options: dict | None = None,
) -> dict:
    options = dict(options or {})
    options["reason"] = reason
    operator_id = require_service_action(admin_user, "onboarding_reset" if mode == "safe" else "test_cleanup", request=options.get("request"))
    options["operator_tg_user_id"] = operator_id
    require_non_operator_target(user_id)
    user = TelegramUser.objects.filter(pk=user_id).first()
    if user is None:
        raise ResetOnboardingError("User was not found.")

    state = get_or_create_admin_state(user)
    _validate_reset_mode(user=user, state=state, admin_user=admin_user, mode=mode, options=options)
    before = _snapshot_user(user, state)
    cleanup_stats = CleanupStats()
    now = timezone.now()

    if options.get("dry_run"):
        return {
            "status": "success",
            "mode": mode,
            "dry_run": True,
            "user_id": user.tg_user_id,
            "allowed": True,
            "cleanup": cleanup_stats.as_dict(),
            "before": before,
        }

    with transaction.atomic():
        if mode in {"cleanup", "full"}:
            cleanup_stats = _cleanup_user_finance_data(user)
        if mode == "replay":
            cleanup_stats = _cleanup_user_replay_data(user)
        if mode == "full":
            cleanup_stats = _cleanup_user_billing_data(user, cleanup_stats)

        user.onboarding_completed = False
        user.onboarding_version = 0
        update_fields = ["onboarding_completed", "onboarding_version"]
        if mode in {"full", "replay"}:
            user.lang = None
            user.base_currency = None
            user.start_date = None
            update_fields.extend(["lang", "base_currency", "start_date"])
        user.save(update_fields=update_fields)

        clear_fsm_state = bool(options.get("clear_fsm_state", True))
        if clear_fsm_state:
            state.current_fsm_state = ""
        state.onboarding_payload = {}
        state.last_user_input = ""
        state.last_bot_response = ""
        state.last_parse_error = ""
        state.last_onboarding_event_at = now
        state.pending_admin_reset_mode = mode if clear_fsm_state else ""
        state.pending_admin_reset_requested_at = now if clear_fsm_state else None
        state.last_admin_reset_at = now
        state_update_fields = [
            "current_fsm_state",
            "onboarding_payload",
            "last_user_input",
            "last_bot_response",
            "last_parse_error",
            "last_onboarding_event_at",
            "pending_admin_reset_mode",
            "pending_admin_reset_requested_at",
            "last_admin_reset_at",
            "updated_at",
        ]
        if mode in {"full", "replay"}:
            state.pending_start_payload = ""
            state_update_fields.append("pending_start_payload")
        if mode == "full":
            state.subscription_status = UserAdminState.SubscriptionStatus.NONE
            state.access_scope = UserAdminState.AccessScope.PAYWALL
            state.access_source = ""
            state_update_fields.extend(
                [
                    "subscription_status",
                    "access_scope",
                    "access_source",
                ]
            )
        state.save(update_fields=state_update_fields)

        event = BotEvent.objects.create(
            user_id=user.tg_user_id,
            event_type="onboarding_reset_by_admin",
            source="admin",
            raw_input=reason[:4000],
            parsed_result={
                "mode": mode,
                "admin_user_id": getattr(admin_user, "pk", None),
                "operator_tg_user_id": options.get("operator_tg_user_id"),
                "reason": reason,
                "cleanup": cleanup_stats.as_dict(),
                "options": {
                    "clear_fsm_state": clear_fsm_state,
                    "send_telegram_notice": bool(options.get("send_telegram_notice")),
                    "send_admin_notification": bool(options.get("send_admin_notification")),
                },
            },
            success=True,
        )

        create_audit_log(
            request=options.get("request"),
            admin_user=admin_user,
            action={
                "safe": "onboarding_reset",
                "cleanup": "test_data_cleanup",
                "full": "full_test_user_reset",
                "replay": "user_data_erased_for_replay",
            }[mode],
            object_type="telegram_user",
            object_id=user.pk,
            target_user_id=user.tg_user_id,
            mode=(
                f"{mode}:miniapp_operator:{options.get('operator_tg_user_id')}"
                if options.get("operator_tg_user_id")
                else mode
            ),
            reason=reason,
            before=before,
            after={
                "cleanup": cleanup_stats.as_dict(),
                "pending_admin_reset_mode": mode,
                "bot_event_id": event.pk,
            },
        )

    if options.get("send_telegram_notice"):
        notice_text = get_bot_setting_value(
            "reset_onboarding_notice_text",
            getattr(settings, "RESET_ONBOARDING_NOTICE_TEXT", "") or "Онбординг скинуто. Тепер напишіть /start у Telegram, щоб пройти його заново.",
        )
        if notice_text.startswith("Р"):
            notice_text = "Онбординг скинуто. Тепер напишіть /start у Telegram, щоб пройти його заново."
        if notice_text:
            _queue_reset_notice(user=user, admin_user=admin_user, text=notice_text)
    if options.get("send_admin_notification"):
        _queue_admin_notice(user=user, mode=mode, reason=reason, cleanup_stats=cleanup_stats.as_dict())

    after = _snapshot_user(user, state)
    return {
        "status": "success",
        "mode": mode,
        "dry_run": False,
        "user_id": user.tg_user_id,
        "cleanup": cleanup_stats.as_dict(),
        "before": before,
        "after": after,
    }


def validate_self_service_account_deletion(user_id: int) -> None:
    with transaction.atomic():
        user = TelegramUser.objects.select_for_update().filter(pk=user_id).first()
        if user is None:
            raise SelfServiceAccountDeletionError("account_not_found", "Account was not found.", status=404)
        existing_tables = _existing_table_names()
        columns_cache: dict[str, set[str]] = {}
        owned_family_ids = _select_ids(
            table_name="families",
            column_name="id",
            where_sql="owner_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        try:
            _validate_family_delete_safety(
                user=user,
                owned_family_ids=owned_family_ids,
                existing_tables=existing_tables,
                cache=columns_cache,
            )
        except HardDeleteUserError as exc:
            raise SelfServiceAccountDeletionError(
                "family_members_must_be_removed",
                "Remove other members from your family space before deleting the account.",
            ) from exc


def delete_own_account(*, user_id: int, request_id: str) -> dict:
    try:
        normalized_request_id = str(UUID(str(request_id)))
    except (TypeError, ValueError) as exc:
        raise SelfServiceAccountDeletionError(
            "deletion_request_invalid",
            "Account deletion request is invalid.",
            status=400,
        ) from exc

    validate_self_service_account_deletion(user_id)
    try:
        from subscriptions.billing import revoke_billing_for_account_deletion
        from subscriptions.monobank import MonobankAPIError

        billing_result = revoke_billing_for_account_deletion(user_id=user_id)
    except MonobankAPIError as exc:
        raise SelfServiceAccountDeletionError(
            "billing_unlink_failed",
            "The saved payment method could not be unlinked. No account data was deleted.",
            status=502,
        ) from exc
    except ValueError as exc:
        raise SelfServiceAccountDeletionError(
            "billing_unlink_failed",
            "Billing could not be safely finalized before account deletion.",
            status=409,
        ) from exc

    try:
        result = hard_delete_user(
            user_id,
            admin_user=None,
            reason="self_service_account_deletion",
            options={
                "_self_service_delete_capability": _SELF_SERVICE_DELETE_CAPABILITY,
                "completion_audit": False,
                "purge_audit_logs": True,
                "self_service_request_id": normalized_request_id,
                "billing_unlink_attempted": bool(billing_result.get("remote_delete_attempted")),
            },
        )
    except HardDeleteUserError as exc:
        raise SelfServiceAccountDeletionError(
            "account_deletion_blocked",
            "Account deletion could not be completed safely.",
        ) from exc

    return {
        "status": "deleted",
        "request_id": normalized_request_id,
        "cleanup": result.get("cleanup", {}),
    }


def hard_delete_user(
    user_id: int,
    *,
    admin_user,
    reason: str,
    options: dict | None = None,
) -> dict:
    options = dict(options or {})
    options["reason"] = reason
    self_service_delete = options.pop("_self_service_delete_capability", None) is _SELF_SERVICE_DELETE_CAPABILITY
    if self_service_delete:
        options["operator_tg_user_id"] = None
    else:
        options["operator_tg_user_id"] = require_service_action(
            admin_user,
            "test_cleanup",
            request=options.get("request"),
        )
        require_non_operator_target(user_id)

    user = TelegramUser.objects.filter(pk=user_id).first()
    if user is None:
        raise HardDeleteUserError("User was not found.")

    state = UserAdminState.objects.filter(telegram_user_id=user.tg_user_id).first()
    if not self_service_delete:
        _validate_hard_delete(user=user, state=state, admin_user=admin_user, options=options)

    snapshot_state = state if state is not None else UserAdminState(telegram_user_id=user.tg_user_id)
    before = _snapshot_user(user, snapshot_state)
    stats = HardDeleteStats()

    if options.get("dry_run"):
        return {
            "status": "success",
            "dry_run": True,
            "user_id": user.tg_user_id,
            "cleanup": stats.as_dict(),
            "before": before,
        }

    with transaction.atomic():
        user = TelegramUser.objects.select_for_update().get(pk=user_id)
        existing_tables = _existing_table_names()
        columns_cache: dict[str, set[str]] = {}
        owned_family_ids = _lock_owned_family_ids(
            user_id=user.tg_user_id,
            existing_tables=existing_tables,
        )
        _validate_family_delete_safety(
            user=user,
            owned_family_ids=owned_family_ids,
            existing_tables=existing_tables,
            cache=columns_cache,
        )

        if self_service_delete:
            stats.anonymized_oidc_token_uses = _update_rows(
                table_name="user_oidc_token_uses",
                set_sql="tg_user_id = 0, provider = '', subject = ''",
                where_sql="tg_user_id = %s",
                params=[user.tg_user_id],
                existing_tables=existing_tables,
                cache=columns_cache,
                required_columns=("tg_user_id", "provider", "subject"),
            )
            stats.anonymized_browser_login_token_uses = _update_rows(
                table_name="miniapp_browser_login_token_uses",
                set_sql="tg_user_id = 0",
                where_sql="tg_user_id = %s",
                params=[user.tg_user_id],
                existing_tables=existing_tables,
                cache=columns_cache,
                required_columns=("tg_user_id",),
            )

        _delete_privacy_identity_rows(
            user_id=user.tg_user_id,
            stats=stats,
            existing_tables=existing_tables,
        )

        stats.deleted_user_tags = _delete_rows(
            table_name="user_tag_links",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_user_push_topics = _delete_rows(
            table_name="user_push_topic_links",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_admin_notes = _delete_rows(
            table_name="admin_notes",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_bot_events = _delete_rows(
            table_name="bot_events",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_admin_message_logs = _delete_rows(
            table_name="admin_message_log",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_broadcast_recipients = _delete_rows(
            table_name="broadcast_recipients",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        if "feedback_items" in existing_tables and "support_cases" in existing_tables:
            stats.deleted_feedback_items = _delete_rows(
                table_name="feedback_items",
                where_sql=(
                    "telegram_user_id = %s "
                    "OR support_case_id IN (SELECT id FROM support_cases WHERE telegram_user_id = %s)"
                ),
                params=[user.tg_user_id, user.tg_user_id],
                existing_tables=existing_tables,
            )
        else:
            stats.deleted_feedback_items = _delete_rows(
                table_name="feedback_items",
                where_sql="telegram_user_id = %s",
                params=[user.tg_user_id],
                existing_tables=existing_tables,
            )
        if "support_messages" in existing_tables and "support_cases" in existing_tables:
            stats.deleted_support_messages = _delete_rows(
                table_name="support_messages",
                where_sql="case_id IN (SELECT id FROM support_cases WHERE telegram_user_id = %s)",
                params=[user.tg_user_id],
                existing_tables=existing_tables,
            )
        stats.deleted_support_cases = _delete_rows(
            table_name="support_cases",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_poll_responses = _delete_rows(
            table_name="poll_responses",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_poll_recipients = _delete_rows(
            table_name="poll_recipients",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )

        stats.deleted_promo_claims = _delete_rows(
            table_name="promo_offer_claims",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_payments = _delete_rows(
            table_name="payments",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_subscription_events = _delete_rows(
            table_name="subscription_events",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_subscriptions = _delete_rows(
            table_name="subscriptions",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_billing_profiles = _delete_rows(
            table_name="billing_profiles",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )

        stats.deleted_ai_transaction_drafts = _delete_rows(
            table_name="ai_transaction_drafts",
            where_sql="tg_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_saving_prompt_settings = _delete_rows(
            table_name="saving_prompt_settings",
            where_sql="tg_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_pending_saving_tasks = _delete_rows(
            table_name="pending_saving_tasks",
            where_sql="tg_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )

        stats.deleted_transactions = _delete_rows(
            table_name="transactions",
            where_sql="tg_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_debt_payments = _delete_rows(
            table_name="debt_payments",
            where_sql="tg_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_debts = _delete_rows(
            table_name="debts",
            where_sql="tg_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )

        if (
            "accounts" in existing_tables
            and _table_has_columns("account_admin_states", "account_id", existing_tables=existing_tables, cache=columns_cache)
        ):
            stats.deleted_account_admin_states = _delete_rows(
                table_name="account_admin_states",
                where_sql="account_id IN (SELECT id FROM accounts WHERE tg_user_id = %s)",
                params=[user.tg_user_id],
                existing_tables=existing_tables,
            )
        stats.deleted_accounts = _delete_rows(
            table_name="accounts",
            where_sql="tg_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        if _table_has_columns("categories", "user_id", "tg_user_id", existing_tables=existing_tables, cache=columns_cache):
            stats.deleted_categories = _delete_rows(
                table_name="categories",
                where_sql="COALESCE(user_id, tg_user_id) = %s OR tg_user_id = %s",
                params=[user.tg_user_id, user.tg_user_id],
                existing_tables=existing_tables,
            )

        stats.nullified_account_created_by = _update_rows(
            table_name="accounts",
            set_sql="created_by_user_id = NULL",
            where_sql="created_by_user_id = %s AND tg_user_id <> %s",
            params=[user.tg_user_id, user.tg_user_id],
            existing_tables=existing_tables,
            cache=columns_cache,
            required_columns=("created_by_user_id", "tg_user_id"),
        )
        stats.nullified_category_created_by = _update_rows(
            table_name="categories",
            set_sql="created_by_user_id = NULL",
            where_sql="created_by_user_id = %s AND COALESCE(user_id, tg_user_id) <> %s",
            params=[user.tg_user_id, user.tg_user_id],
            existing_tables=existing_tables,
            cache=columns_cache,
            required_columns=("created_by_user_id", "user_id", "tg_user_id"),
        )
        stats.nullified_transaction_created_by = _update_rows(
            table_name="transactions",
            set_sql="created_by_user_id = NULL",
            where_sql="created_by_user_id = %s AND tg_user_id <> %s",
            params=[user.tg_user_id, user.tg_user_id],
            existing_tables=existing_tables,
            cache=columns_cache,
            required_columns=("created_by_user_id", "tg_user_id"),
        )
        stats.nullified_transaction_updated_by = _update_rows(
            table_name="transactions",
            set_sql="updated_by_user_id = NULL",
            where_sql="updated_by_user_id = %s AND tg_user_id <> %s",
            params=[user.tg_user_id, user.tg_user_id],
            existing_tables=existing_tables,
            cache=columns_cache,
            required_columns=("updated_by_user_id", "tg_user_id"),
        )
        stats.nullified_transaction_deleted_by = _update_rows(
            table_name="transactions",
            set_sql="deleted_by_user_id = NULL",
            where_sql="deleted_by_user_id = %s AND tg_user_id <> %s",
            params=[user.tg_user_id, user.tg_user_id],
            existing_tables=existing_tables,
            cache=columns_cache,
            required_columns=("deleted_by_user_id", "tg_user_id"),
        )
        stats.nullified_debt_borrowers = _update_rows(
            table_name="debts",
            set_sql="borrower_user_id = NULL",
            where_sql="borrower_user_id = %s AND tg_user_id <> %s",
            params=[user.tg_user_id, user.tg_user_id],
            existing_tables=existing_tables,
            cache=columns_cache,
            required_columns=("borrower_user_id", "tg_user_id"),
        )

        stats.nullified_family_member_invited_by = _update_rows(
            table_name="family_members",
            set_sql="invited_by_user_id = NULL",
            where_sql="invited_by_user_id = %s AND user_id <> %s",
            params=[user.tg_user_id, user.tg_user_id],
            existing_tables=existing_tables,
            cache=columns_cache,
            required_columns=("invited_by_user_id", "user_id"),
        )
        stats.nullified_family_invite_used_by = _update_rows(
            table_name="family_invites",
            set_sql="used_by_user_id = NULL",
            where_sql="used_by_user_id = %s AND created_by_user_id <> %s",
            params=[user.tg_user_id, user.tg_user_id],
            existing_tables=existing_tables,
            cache=columns_cache,
            required_columns=("used_by_user_id", "created_by_user_id"),
        )

        stats.deleted_debt_invites = _delete_rows(
            table_name="debt_invites",
            where_sql="created_by_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )

        if owned_family_ids:
            placeholders, placeholder_params = _sql_placeholders(owned_family_ids)
            stats.deleted_family_invites += _delete_rows(
                table_name="family_invites",
                where_sql=f"family_id IN ({placeholders})",
                params=placeholder_params,
                existing_tables=existing_tables,
            )
            stats.deleted_family_members += _delete_rows(
                table_name="family_members",
                where_sql=f"family_id IN ({placeholders})",
                params=placeholder_params,
                existing_tables=existing_tables,
            )

        stats.deleted_family_invites += _delete_rows(
            table_name="family_invites",
            where_sql="created_by_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_family_members += _delete_rows(
            table_name="family_members",
            where_sql="user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_families = _delete_rows(
            table_name="families",
            where_sql="owner_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )

        if options.get("purge_audit_logs", True):
            stats.deleted_audit_logs = _delete_rows(
                table_name="admin_audit_logs",
                where_sql="target_user_id = %s OR (object_type = %s AND object_id = %s)",
                params=[user.tg_user_id, "telegram_user", str(user.pk)],
                existing_tables=existing_tables,
            )

        stats.deleted_admin_states = _delete_rows(
            table_name="user_admin_states",
            where_sql="telegram_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )
        stats.deleted_user_rows = _delete_rows(
            table_name="users",
            where_sql="tg_user_id = %s",
            params=[user.tg_user_id],
            existing_tables=existing_tables,
        )

        if stats.deleted_user_rows != 1:
            raise HardDeleteUserError("Hard delete не зміг видалити рядок користувача з таблиці users.")

        if self_service_delete:
            create_audit_log(
                request=None,
                admin_user=None,
                action="account_self_deleted",
                object_type="privacy_request",
                object_id=options["self_service_request_id"],
                target_user_id=None,
                mode="self_service",
                reason="user_initiated",
                before={},
                after={
                    "deleted": True,
                    "billing_unlink_attempted": bool(options.get("billing_unlink_attempted")),
                },
            )
        elif options.get("completion_audit", True):
            create_audit_log(
                request=options.get("request"),
                admin_user=admin_user,
                action="telegram_user_hard_deleted",
                object_type="telegram_user",
                object_id=user.pk,
                target_user_id=user.tg_user_id,
                mode=(
                    f"hard_delete:miniapp_operator:{options.get('operator_tg_user_id')}"
                    if options.get("operator_tg_user_id")
                    else "hard_delete"
                ),
                reason=reason,
                before={},
                after={
                    "deleted": True,
                    "cleanup": stats.as_dict(),
                },
            )

    return {
        "status": "success",
        "dry_run": False,
        "user_id": user.tg_user_id,
        "cleanup": stats.as_dict(),
        "before": before,
        "after": {
            "deleted": True,
        },
    }
