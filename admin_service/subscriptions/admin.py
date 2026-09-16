from __future__ import annotations

from decimal import Decimal
import json
import uuid

from django import forms
from django.conf import settings
from django.core import signing
from django.db import transaction
from django.http import HttpResponse
from django.contrib import admin, messages
from django.contrib.admin.helpers import ACTION_CHECKBOX_NAME
from django.shortcuts import get_object_or_404, redirect
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils import timezone
from django.utils.html import format_html

from common.admin import AuditedModelAdmin, HiddenFromMenuAdminMixin, OperationalModelAdmin
from common.admin_actions import admin_action_permission, require_admin_action
from common.admin_site import admin_site
from common.admin_ui import boolean_badge, money_value, status_badge, user_identity
from common.audit import create_audit_log
from subscriptions.billing import retry_monobank_charge, resolve_unknown_monobank_charge
from subscriptions.models import (
    BillingProfile,
    Payment,
    Plan,
    PromoOffer,
    PromoOfferClaim,
    Subscription,
    SubscriptionEvent,
    TrialRecoveryCampaign,
    TrialRecoveryDelivery,
    TrialRecoveryRecipient,
)
from subscriptions.monobank import MonobankAPIError
from subscriptions.payloads import sanitize_monobank_payload
from subscriptions.recovery_reporting import build_recovery_campaign_dashboard
from subscriptions.services import confirm_payment, sync_admin_subscription_state
from subscriptions.trial_recovery import (
    audience_preview,
    campaign_metrics,
    campaign_uses_registered_without_card_audience,
    expand_campaign_audience,
    launch_campaign,
    pause_campaign,
)

BillingProfile._meta.verbose_name = "Прив'язана картка"
BillingProfile._meta.verbose_name_plural = "Прив'язані картки"

Plan._meta.verbose_name = "Тарифний план"
Plan._meta.verbose_name_plural = "Тарифні плани"
Subscription._meta.verbose_name = "Підписка"
Subscription._meta.verbose_name_plural = "Підписки"
Payment._meta.verbose_name = "Платіж"
Payment._meta.verbose_name_plural = "Платежі"

@admin.register(Plan, site=admin_site)
class PlanAdmin(AuditedModelAdmin):
    audit_object_type = "plan"
    list_display = ("name", "price_display", "duration_display", "active_display", "public_display", "archive_display", "display_order")
    search_fields = ("name", "slug", "description")
    list_filter = ("is_active", "is_public", "is_archived", "currency")
    readonly_fields = ("created_at", "updated_at")
    fieldsets = (
        ("Тариф", {"fields": ("name", "slug", "price", "currency", "duration_days", "description", "features")}),
        ("Публікація", {"fields": ("is_active", "is_public", "is_archived", "display_order")}),
        ("Службове", {"fields": ("created_at", "updated_at")}),
    )

    @admin.display(description="Ціна", ordering="price")
    def price_display(self, obj: Plan):
        return money_value(obj.price, obj.currency)

    @admin.display(description="Тривалість", ordering="duration_days")
    def duration_display(self, obj: Plan):
        return f"{obj.duration_days} днів"

    @admin.display(description="Стан", ordering="is_active")
    def active_display(self, obj: Plan):
        return boolean_badge(obj.is_active, true_label="Активний", false_label="Неактивний")

    @admin.display(description="Публічний", ordering="is_public")
    def public_display(self, obj: Plan):
        return boolean_badge(obj.is_public)

    @admin.display(description="Архів", ordering="is_archived")
    def archive_display(self, obj: Plan):
        return boolean_badge(obj.is_archived, true_label="В архіві", false_label="Ні")


@admin.register(Subscription, site=admin_site)
class SubscriptionAdmin(AuditedModelAdmin):
    audit_object_type = "subscription"
    list_display = (
        "user_display",
        "plan_display",
        "status_display",
        "amount_display",
        "auto_renew_display",
        "expires_at",
        "next_charge_at",
        "source_display",
    )
    list_display_links = ("status_display",)
    search_fields = ("user__username", "user__tg_user_id", "plan", "payment_id")
    list_filter = ("status", "source", "plan_ref", "started_at", "expires_at")
    list_per_page = 25
    readonly_fields = ("created_at", "updated_at")
    actions = None

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("user", "plan_ref")

    @admin.display(description="Користувач", ordering="user__username")
    def user_display(self, obj: Subscription):
        return user_identity(obj.user)

    @admin.display(description="Тариф", ordering="plan")
    def plan_display(self, obj: Subscription):
        return getattr(obj.plan_ref, "name", None) or obj.plan or "—"

    @admin.display(description="Статус", ordering="status")
    def status_display(self, obj: Subscription):
        labels = {
            "trial": "Пробний період",
            "active": "Активна",
            "paid": "Оплачена",
            "expired": "Завершена",
            "cancelled": "Скасована",
            "manual": "Ручна",
            "lifetime": "Безстрокова",
        }
        return status_badge(obj.status, labels.get(obj.status, obj.status or "Невідомо"))

    @admin.display(description="Сума", ordering="amount")
    def amount_display(self, obj: Subscription):
        return money_value(obj.amount, obj.currency) if obj.amount is not None else "—"

    @admin.display(description="Автоподовження", ordering="auto_renew")
    def auto_renew_display(self, obj: Subscription):
        return boolean_badge(obj.auto_renew, true_label="Увімкнено", false_label="Вимкнено")

    @admin.display(description="Джерело", ordering="source")
    def source_display(self, obj: Subscription):
        labels = {"system": "Система", "payment": "Оплата", "admin": "Адміністратор", "promo": "Промо"}
        return labels.get(obj.source, obj.source or "—")

    def save_model(self, request, obj, form, change):
        obj.updated_by = request.user
        if not obj.created_by_id:
            obj.created_by = request.user
        super().save_model(request, obj, form, change)
        sync_admin_subscription_state(obj.user_id, subscription=obj)


@admin.action(description="Підтвердити оплату вручну")
def confirm_manually(modeladmin, request, queryset):
    require_admin_action(getattr(request, "user", None), "payment_change")
    if request.POST.get("confirm_action") != "1":
        return _payment_action_confirmation(
            request,
            queryset,
            title="Підтвердити вибрані платежі вручну?",
            warning="Ця дія активує або продовжить підписку для кожного валідного платежу та запише операцію в audit log.",
            submit_label="Підтвердити платежі",
        )
    for payment in queryset.select_related("user", "plan", "subscription"):
        try:
            subscription = confirm_payment(payment=payment, admin_user=request.user)
        except ValueError as exc:
            messages.warning(request, f"Платіж #{payment.pk}: {exc}")
            continue
        create_audit_log(
            request=request,
            admin_user=request.user,
            action="payment confirmed manually",
            object_type="payment",
            object_id=payment.pk,
            before={"status": Payment.Status.PENDING},
            after={"status": payment.status, "subscription_id": subscription.pk},
        )
    messages.success(request, "Ручне підтвердження платежів завершено.")


@admin.action(description="Відхилити платіж")
def reject_payment(modeladmin, request, queryset):
    require_admin_action(getattr(request, "user", None), "payment_change")
    if request.POST.get("confirm_action") != "1":
        return _payment_action_confirmation(
            request,
            queryset,
            title="Відхилити вибрані платежі?",
            warning="Успішні та вже підтверджені платежі не будуть змінені. Решта отримають статус «Відхилено».",
            submit_label="Відхилити платежі",
        )
    updated = queryset.exclude(status__in=[Payment.Status.PAID, Payment.Status.MANUAL_CONFIRMED]).update(
        status=Payment.Status.REJECTED,
        updated_at=timezone.now(),
        confirmed_by_id=request.user.pk,
    )
    messages.success(request, f"Відхилено платежів: {updated}.")


@admin.action(description="Позначити поверненим (без виклику Monobank)")
def mark_refunded(modeladmin, request, queryset):
    require_admin_action(getattr(request, "user", None), "payment_change")
    if request.POST.get("confirm_action") != "1":
        return _payment_action_confirmation(
            request,
            queryset,
            title="Позначити вибрані платежі поверненими?",
            warning="Увага: це лише змінить внутрішній статус. Запит на реальне повернення коштів у Monobank не виконується.",
            submit_label="Позначити поверненими",
        )
    updated = queryset.update(status=Payment.Status.REFUNDED, updated_at=timezone.now(), confirmed_by_id=request.user.pk)
    messages.success(request, f"Позначено поверненими: {updated}. Запит до Monobank не виконувався.")


class UnknownChargeResolutionForm(forms.Form):
    reason = forms.CharField(label="Provider evidence / reconciliation reference", max_length=2000, widget=forms.Textarea)
    invoice_id = forms.CharField(required=False, max_length=255, label="Verified provider invoiceId")
    confirmed_no_charge = forms.BooleanField(required=False, label="Provider evidence confirms NO debit (not merely a timeout)")
    preview_token = forms.CharField(required=False, widget=forms.HiddenInput)
    confirm = forms.BooleanField(required=False, label="Confirm this exact reconciliation; this does not initiate a debit")

    def clean(self):
        data = super().clean()
        if bool(data.get("invoice_id")) == bool(data.get("confirmed_no_charge")):
            raise forms.ValidationError("Supply exactly one invoiceId OR an explicit no-debit attestation.")
        if data.get("preview_token") and not data.get("confirm"):
            raise forms.ValidationError("Explicit confirmation is required after preview.")
        return data


class ForceChargeNowForm(forms.Form):
    intent_key = forms.CharField(required=True, widget=forms.HiddenInput)
    confirm = forms.BooleanField(
        required=True,
        label="Підтверджую, що треба негайно запустити списання із збереженої картки.",
    )
    reason = forms.CharField(
        required=False,
        label="Коментар для аудиту",
        widget=forms.Textarea(attrs={"rows": 3}),
        help_text="Необов'язково. Потрапить в admin audit log.",
    )


def _payment_action_confirmation(request, queryset, *, title: str, warning: str, submit_label: str):
    return TemplateResponse(
        request,
        "admin/payment_action_confirmation.html",
        {
            **admin_site.each_context(request),
            "title": title,
            "warning": warning,
            "submit_label": submit_label,
            "payments": queryset.select_related("user").order_by("-created_at")[:50],
            "selected_count": queryset.count(),
            "selected_ids": request.POST.getlist(ACTION_CHECKBOX_NAME),
            "action_name": request.POST.get("action", ""),
            "action_checkbox_name": ACTION_CHECKBOX_NAME,
            "select_across": request.POST.get("select_across", "0"),
            "opts": Payment._meta,
        },
    )


@admin.register(Payment, site=admin_site)
class PaymentAdmin(AuditedModelAdmin):
    audit_object_type = "payment"
    list_display = (
        "user_display",
        "kind_display",
        "amount_display",
        "status_display",
        "plan",
        "provider_display",
        "paid_at",
        "created_at",
    )
    list_display_links = ("status_display",)
    search_fields = ("user__username", "user__tg_user_id", "provider_payment_id", "external_transaction_id")
    list_filter = ("kind", "status", "provider", "currency", "plan", "created_at", "paid_at")
    readonly_fields = (
        "user",
        "subscription",
        "plan",
        "provider",
        "provider_payment_id",
        "external_transaction_id",
        "amount",
        "currency",
        "status",
        "kind",
        "action_url",
        "screenshot",
        "paid_at",
        "provider_modified_at",
        "confirmed_by",
        "created_at",
        "updated_at",
        "sanitized_raw_payload",
        "unknown_charge_resolution",
    )
    exclude = ("raw_payload",)
    actions = (confirm_manually, reject_payment, mark_refunded)
    fieldsets = (
        ("Платіж", {"fields": ("user", "subscription", "plan", "kind", "status", "amount", "currency")}),
        ("Провайдер", {"fields": ("provider", "provider_payment_id", "external_transaction_id", "action_url", "provider_modified_at")}),
        ("Підтвердження", {"fields": ("paid_at", "confirmed_by", "admin_comment", "screenshot")}),
        ("Безпечна технічна відповідь", {"fields": ("sanitized_raw_payload", "unknown_charge_resolution")}),
        ("Службове", {"fields": ("created_at", "updated_at")}),
    )

    def has_add_permission(self, request):
        return False

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("user", "plan", "confirmed_by")

    @admin.display(description="Користувач", ordering="user__username")
    def user_display(self, obj: Payment):
        return user_identity(obj.user)

    @admin.display(description="Тип", ordering="kind")
    def kind_display(self, obj: Payment):
        labels = {"bind": "Прив'язка картки", "renewal": "Продовження", "retry": "Повторна спроба", "legacy": "Імпортований"}
        return labels.get(obj.kind, obj.kind or "—")

    @admin.display(description="Сума", ordering="amount")
    def amount_display(self, obj: Payment):
        return money_value(obj.amount, obj.currency)

    @admin.display(description="Статус", ordering="status")
    def status_display(self, obj: Payment):
        labels = {
            "created": "Створено",
            "pending": "Очікує",
            "paid": "Успішно",
            "failed": "Помилка",
            "rejected": "Відхилено",
            "refunded": "Повернено",
            "manual_confirmed": "Підтверджено вручну",
        }
        return status_badge(obj.status, labels.get(obj.status, obj.status or "Невідомо"))

    @admin.display(description="Провайдер", ordering="provider")
    def provider_display(self, obj: Payment):
        return "Monobank" if str(obj.provider or "").lower() == "monobank" else (obj.provider or "—")

    def get_urls(self):
        return [path("<int:object_id>/resolve-unknown/", self.admin_site.admin_view(self.resolve_unknown_view),
                     name="subscriptions_payment_resolve_unknown")] + super().get_urls()

    @admin.display(description="Unknown charge reconciliation")
    def unknown_charge_resolution(self, obj):
        if obj.status != Payment.Status.PENDING or obj.provider_payment_id or obj.provider != "monobank":
            return "Not an invoice-less pending charge."
        return format_html('<a class="button" href="{}">Preview reconciliation</a>',
            reverse("admin:subscriptions_payment_resolve_unknown", args=[obj.pk]))

    @admin_action_permission("payment_change")
    def resolve_unknown_view(self, request, object_id):
        payment = get_object_or_404(Payment, pk=object_id, provider="monobank")
        if payment.status != Payment.Status.PENDING or payment.provider_payment_id:
            return HttpResponse("This attempt is already resolved. Refresh its current state.", status=409)
        form = UnknownChargeResolutionForm(request.POST or None)
        submit = "Preview reconciliation"
        if request.method == "POST" and form.is_valid():
            data = form.cleaned_data
            evidence = {key:data[key] for key in ("reason", "invoice_id", "confirmed_no_charge")}
            bound = {"payment_id":payment.pk, "admin_user_id":request.user.pk,
                     "updated_at":payment.updated_at.isoformat(), **evidence}
            if not data["preview_token"]:
                token = signing.dumps(bound, salt="billing-unknown-preview-v1")
                form = UnknownChargeResolutionForm(initial={**evidence, "preview_token":token})
                submit = "Confirm reconciliation"
            else:
                try:
                    preview = signing.loads(data["preview_token"], salt="billing-unknown-preview-v1", max_age=600)
                    if preview != bound:
                        return HttpResponse("Stale or changed reconciliation preview.", status=409)
                    with transaction.atomic():
                        from users.models import TelegramUser
                        TelegramUser.objects.select_for_update().get(pk=payment.user_id)
                        current = Payment.objects.get(pk=payment.pk)
                        if current.updated_at.isoformat() != preview["updated_at"]:
                            return HttpResponse("Reconciliation changed; preview again.", status=409)
                        result = resolve_unknown_monobank_charge(payment_id=payment.pk, admin_user=request.user, **evidence)
                        create_audit_log(request=request, admin_user=request.user, action="unknown_charge_reconciled",
                            object_type="payment", object_id=payment.pk, before={"status":"pending"}, after=result)
                except (signing.BadSignature, ValueError, MonobankAPIError) as exc:
                    form.add_error(None, str(exc))
                else:
                    return redirect(reverse("admin:subscriptions_payment_change", args=[payment.pk]))
        return TemplateResponse(request, "admin/object_action_form.html", {
            **self.admin_site.each_context(request), "title":"Resolve unknown Monobank charge", "opts":self.model._meta,
            "object":payment, "form":form, "submit_label":submit,
            "help_text":f"Payment #{payment.pk}: {payment.amount} {payment.currency}. No new debit. Confirm provider evidence; never guess a timeout means no charge.",
            "back_url":reverse("admin:subscriptions_payment_change", args=[payment.pk]),
        })

    @admin.display(description="Sanitized payload")
    def sanitized_raw_payload(self, obj: Payment):
        payload = sanitize_monobank_payload(obj.raw_payload or {})
        if not payload:
            return "-"
        return format_html("<pre>{}</pre>", json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


@admin.register(BillingProfile, site=admin_site)
class BillingProfileAdmin(AuditedModelAdmin):
    audit_object_type = "billing_profile"
    list_display = (
        "user_display",
        "masked_pan",
        "card_saved_display",
        "status_display",
        "auto_renew_display",
        "last_charge_display",
        "last_bound_at",
        "last_charge_at",
    )
    list_display_links = ("masked_pan",)
    search_fields = ("user__username", "user__tg_user_id", "wallet_id", "masked_pan")
    list_filter = ("provider", "status", "auto_renew_enabled", "last_charge_status")
    readonly_fields = (
        "user",
        "provider",
        "wallet_id",
        "masked_pan",
        "status",
        "auto_renew_enabled",
        "last_charge_status",
        "last_failure_reason",
        "last_action_url",
        "last_bound_at",
        "last_charge_at",
        "created_at",
        "updated_at",
        "token_present",
        "force_charge_now",
    )
    exclude = ("card_token",)
    actions = None
    fieldsets = (
        ("Користувач і картка", {"fields": ("user", "provider", "masked_pan", "token_present", "status")}),
        ("Автоподовження", {"fields": ("auto_renew_enabled", "last_charge_status", "last_failure_reason", "last_bound_at", "last_charge_at", "force_charge_now")}),
        ("Технічне", {"fields": ("wallet_id", "last_action_url", "created_at", "updated_at"), "classes": ("collapse",)}),
    )

    def has_add_permission(self, request):
        return False

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("user")

    @admin.display(description="Користувач", ordering="user__username")
    def user_display(self, obj: BillingProfile):
        return user_identity(obj.user)

    @admin.display(description="Картка збережена")
    def card_saved_display(self, obj: BillingProfile):
        return boolean_badge(bool(obj.card_token))

    @admin.display(description="Статус", ordering="status")
    def status_display(self, obj: BillingProfile):
        labels = {
            "pending": "Очікує",
            "active": "Активна",
            "action_required": "Потрібна дія",
            "failed": "Помилка",
            "cancelled": "Скасована",
            "missing_token": "Немає token",
        }
        return status_badge(obj.status, labels.get(obj.status, obj.status or "Невідомо"))

    @admin.display(description="Автоподовження", ordering="auto_renew_enabled")
    def auto_renew_display(self, obj: BillingProfile):
        return boolean_badge(obj.auto_renew_enabled, true_label="Увімкнено", false_label="Вимкнено")

    @admin.display(description="Останнє списання", ordering="last_charge_status")
    def last_charge_display(self, obj: BillingProfile):
        return status_badge(obj.last_charge_status, obj.last_charge_status or "Ще не було")

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                "<path:object_id>/force-charge/",
                self.admin_site.admin_view(self.force_charge_view),
                name="subscriptions_billingprofile_force_charge",
            ),
        ]
        return custom_urls + urls

    @admin.display(boolean=True, description="Token saved")
    def token_present(self, obj: BillingProfile) -> bool:
        return bool(obj.card_token)

    @admin.display(description="Charge now")
    def force_charge_now(self, obj: BillingProfile):
        if not getattr(obj, "pk", None):
            return "Збережіть профіль, щоб відкрити операторську дію."
        if not obj.card_token:
            return "Недоступно: немає збереженого card token."
        if not obj.auto_renew_enabled:
            return "Недоступно: автооновлення вимкнено."
        url = reverse("admin:subscriptions_billingprofile_force_charge", args=[obj.pk])
        return format_html("<a class='button' href='{}'>Форснути оплату зараз</a>", url)

    @admin_action_permission("payment_change")
    def force_charge_view(self, request, object_id):
        profile = get_object_or_404(BillingProfile.objects.select_related("user"), pk=object_id)
        back_url = reverse("admin:subscriptions_billingprofile_change", args=[profile.pk])
        latest_subscription = Subscription.objects.filter(user_id=profile.user_id).order_by("-created_at").first()

        if not profile.card_token:
            messages.error(request, "Не можна запустити списання: для користувача немає збереженого card token.")
            return redirect(back_url)
        if not profile.auto_renew_enabled:
            messages.error(request, "Не можна запустити списання: автооновлення для цього профілю вимкнено.")
            return redirect(back_url)

        form = ForceChargeNowForm(
            request.POST or None,
            initial={"intent_key": f"admin-force-{uuid.uuid4().hex}"},
        )
        if request.method == "POST" and form.is_valid():
            before = {
                "profile_status": profile.status,
                "auto_renew_enabled": profile.auto_renew_enabled,
                "last_charge_status": profile.last_charge_status,
                "subscription_status": latest_subscription.status if latest_subscription else "",
                "subscription_expires_at": latest_subscription.expires_at if latest_subscription else None,
                "subscription_next_charge_at": latest_subscription.next_charge_at if latest_subscription else None,
            }
            try:
                intent_key = str(form.cleaned_data.get("intent_key") or "").strip()
                result = retry_monobank_charge(
                    user_id=profile.user_id,
                    intent_key=intent_key,
                )
            except (ValueError, MonobankAPIError) as exc:
                messages.error(request, f"Не вдалося запустити списання: {exc}")
            else:
                payment = Payment.objects.filter(pk=result.get("payment_id")).first()
                payment_status = str(payment.status if payment is not None else (result.get("status") or ""))
                action_url = str(payment.action_url if payment is not None else (result.get("action_url") or ""))
                create_audit_log(
                    request=request,
                    admin_user=request.user,
                    action="billing_force_charge_now",
                    object_type="billing_profile",
                    object_id=profile.pk,
                    target_user_id=profile.user_id,
                    mode="force_charge_now",
                    reason=form.cleaned_data.get("reason", ""),
                    before=before,
                    after={
                        "payment_id": result.get("payment_id"),
                        "invoice_id": result.get("invoice_id"),
                        "payment_status": payment_status,
                        "action_url": action_url,
                    },
                )
                if action_url:
                    messages.warning(
                        request,
                        "Списання запущено, але Monobank просить додаткове підтвердження. Перевірте action URL в останньому платежі.",
                    )
                elif payment_status == Payment.Status.PAID:
                    messages.success(request, "Списання успішно виконано.")
                else:
                    messages.success(request, f"Спробу списання створено. Поточний статус: {payment_status or 'pending'}.")
                return redirect(back_url)

        renewal_amount = (Decimal(settings.MONO_RENEWAL_AMOUNT) / Decimal("100")).quantize(Decimal("0.01"))
        help_text = (
            f"Це вручну запустить списання {renewal_amount} UAH через Monobank token payment "
            "для цього користувача просто зараз. У billing history з'явиться окремий платіж виду retry."
        )
        context = {
            **self.admin_site.each_context(request),
            "title": "Форснути оплату зараз",
            "opts": self.model._meta,
            "form": form,
            "profile": profile,
            "subscription": latest_subscription,
            "renewal_amount": renewal_amount,
            "help_text": help_text,
            "submit_label": "Списати зараз",
            "back_url": back_url,
        }
        return TemplateResponse(request, "admin/billing_profile_force_charge.html", context)


@admin.register(SubscriptionEvent, site=admin_site)
class SubscriptionEventAdmin(HiddenFromMenuAdminMixin, AuditedModelAdmin):
    audit_object_type = "subscription_event"
    list_display = ("subscription", "user", "event_type", "created_at")
    search_fields = ("user__username", "user__tg_user_id", "event_type")
    list_filter = ("event_type", "created_at")
    readonly_fields = ("created_at",)


@admin.register(PromoOffer, site=admin_site)
class PromoOfferAdmin(AuditedModelAdmin):
    audit_object_type = "promo_offer"
    list_display = ("code", "label", "trial_days", "source", "is_active", "max_uses", "starts_at", "ends_at", "created_at")
    search_fields = ("code", "label", "source")
    list_filter = ("is_active", "trial_days", "source")
    readonly_fields = ("created_at", "updated_at")


@admin.register(PromoOfferClaim, site=admin_site)
class PromoOfferClaimAdmin(HiddenFromMenuAdminMixin, AuditedModelAdmin):
    audit_object_type = "promo_offer_claim"
    list_display = ("offer", "user", "status", "bind_payment", "subscription", "consumed_at", "created_at")
    search_fields = ("offer__code", "user__username", "user__tg_user_id")
    list_filter = ("status", "offer")
    readonly_fields = ("created_at", "updated_at", "consumed_at")
    actions = None


class TrialRecoveryLaunchForm(forms.Form):
    confirm = forms.BooleanField(
        required=True,
        label="Підтверджую запуск recovery-кампанії для показаної аудиторії",
        help_text="Без цієї окремої згоди кампанія залишиться чернеткою, а повідомлення не надсилатимуться.",
    )


class TrialRecoveryPauseForm(forms.Form):
    confirm = forms.BooleanField(
        required=True,
        label="Підтверджую призупинення recovery-кампанії",
    )


class TrialRecoveryExpandForm(forms.Form):
    confirm = forms.BooleanField(
        required=True,
        label="Підтверджую додавання всіх показаних користувачів без успішної оплати та картки",
        help_text="Без цього підтвердження активна кампанія не зміниться й нова хвиля не буде запланована.",
    )


TRIAL_RECOVERY_EXCLUSION_LABELS = {
    "has_successful_payment": "Уже мають успішну оплату",
    "blocked": "Заблоковані",
    "cannot_receive_messages": "Не можуть отримувати повідомлення",
    "test_user": "Тестові користувачі",
    "has_card": "Картку вже прив’язано",
    "bind_in_progress": "Оплата 1 грн уже в процесі",
    "already_enrolled": "Уже є у recovery",
}


def _trial_recovery_exclusion_rows(preview):
    return [
        {"label": TRIAL_RECOVERY_EXCLUSION_LABELS.get(reason, reason), "count": count}
        for reason, count in preview["excluded"].items()
    ]


@admin.register(TrialRecoveryCampaign, site=admin_site)
class TrialRecoveryCampaignAdmin(AuditedModelAdmin):
    audit_object_type = "trial_recovery_campaign"
    change_form_template = "admin/subscriptions/trialrecoverycampaign/change_form.html"
    list_display = ("name", "status_display", "coverage_display", "responses_display", "contacts_display", "conversions_display", "launch_at", "created_at")
    list_filter = ("status", "created_at", "launch_at")
    search_fields = ("name",)
    actions = None
    readonly_fields = (
        "status",
        "launch_at",
        "started_at",
        "completed_at",
        "launched_by",
        "audience_snapshot_display",
        "campaign_controls",
        "created_at",
        "updated_at",
    )
    fieldsets = (
        ("Налаштування кампанії", {"fields": ("name", "status", "fallback_timezone", "send_window_start", "send_window_end")} ),
        ("Запуск", {"fields": ("launch_at", "started_at", "launched_by")} ),
        ("Службове", {"fields": ("audience_snapshot_display", "completed_at", "created_at", "updated_at"), "classes": ("collapse",)}),
    )

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("launched_by")

    @admin_action_permission("recovery_view")
    def change_view(self, request, object_id, form_url="", extra_context=None):
        campaign = get_object_or_404(TrialRecoveryCampaign, pk=object_id)
        context = {
            "recovery_dashboard": build_recovery_campaign_dashboard(campaign),
        }
        if extra_context:
            context.update(extra_context)
        return super().change_view(request, object_id, form_url, extra_context=context)

    def get_readonly_fields(self, request, obj=None):
        fields = list(super().get_readonly_fields(request, obj))
        if obj is not None and obj.status != TrialRecoveryCampaign.Status.DRAFT:
            fields.extend(["name", "fallback_timezone", "send_window_start", "send_window_end"])
        return tuple(dict.fromkeys(fields))

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path("<path:object_id>/launch/", self.admin_site.admin_view(self.launch_view), name="subscriptions_trialrecoverycampaign_launch"),
            path("<path:object_id>/expand-audience/", self.admin_site.admin_view(self.expand_view), name="subscriptions_trialrecoverycampaign_expand"),
            path("<path:object_id>/pause/", self.admin_site.admin_view(self.pause_view), name="subscriptions_trialrecoverycampaign_pause"),
        ]
        return custom_urls + urls

    @admin.display(description="Статус", ordering="status")
    def status_display(self, obj):
        labels = {
            TrialRecoveryCampaign.Status.DRAFT: "Чернетка",
            TrialRecoveryCampaign.Status.SCHEDULED: "Запланована",
            TrialRecoveryCampaign.Status.RUNNING: "Працює",
            TrialRecoveryCampaign.Status.PAUSED: "Призупинена",
            TrialRecoveryCampaign.Status.COMPLETED: "Завершена",
        }
        return status_badge(obj.status, labels.get(obj.status, obj.status))

    @admin.display(description="У воронці")
    def coverage_display(self, obj):
        return campaign_metrics(obj)["recipients"]

    @admin.display(description="Відповіді")
    def responses_display(self, obj):
        return campaign_metrics(obj)["responses"]

    @admin.display(description="Контакти")
    def contacts_display(self, obj):
        return campaign_metrics(obj)["contact_requests"]

    @admin.display(description="Відновлено")
    def conversions_display(self, obj):
        return campaign_metrics(obj)["recovered_users"]

    @admin.display(description="Керування")
    def campaign_controls(self, obj):
        if not getattr(obj, "pk", None):
            return "Спершу збережіть чернетку."
        if obj.status == TrialRecoveryCampaign.Status.DRAFT:
            url = reverse("admin:subscriptions_trialrecoverycampaign_launch", args=[obj.pk])
            return format_html("<a class='button' href='{}'>Перевірити й запустити кампанію</a>", url)
        if obj.status in {TrialRecoveryCampaign.Status.SCHEDULED, TrialRecoveryCampaign.Status.RUNNING}:
            url = reverse("admin:subscriptions_trialrecoverycampaign_pause", args=[obj.pk])
            return format_html("<a class='button' href='{}'>Призупинити кампанію</a>", url)
        return "Кампанія не надсилає повідомлення."

    @admin.display(description="Знімок аудиторії на момент запуску")
    def audience_snapshot_display(self, obj):
        value = obj.audience_snapshot or {}
        return format_html("<pre>{}</pre>", json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)) if value else "Ще не запускалася."

    @admin.display(description="Поточні метрики")
    def metrics_display(self, obj):
        if not getattr(obj, "pk", None):
            return "—"
        return format_html("<pre>{}</pre>", json.dumps(campaign_metrics(obj), ensure_ascii=False, indent=2, sort_keys=True))

    @admin_action_permission("recovery_change")
    def launch_view(self, request, object_id):
        campaign = get_object_or_404(TrialRecoveryCampaign, pk=object_id)
        back_url = reverse("admin:subscriptions_trialrecoverycampaign_change", args=[campaign.pk])
        if campaign.status != TrialRecoveryCampaign.Status.DRAFT:
            messages.error(request, "Запустити можна лише кампанію зі статусом «Чернетка».")
            return redirect(back_url)
        preview = audience_preview()
        form = TrialRecoveryLaunchForm(request.POST or None)
        if request.method == "POST" and form.is_valid():
            try:
                result = launch_campaign(campaign, launch_at=timezone.now(), admin_user=request.user)
            except ValueError as exc:
                messages.error(request, str(exc))
            else:
                create_audit_log(
                    request=request,
                    admin_user=request.user,
                    action="trial_recovery_campaign_launched",
                    object_type="trial_recovery_campaign",
                    object_id=campaign.pk,
                    before={"status": TrialRecoveryCampaign.Status.DRAFT},
                    after={"status": campaign.status, **result},
                )
                messages.success(request, f"Кампанію запущено. Заплановано отримувачів: {result['created_recipients']}. Доставка відбудеться лише з 10:00 до 19:00 локального часу.")
                return redirect(back_url)
        return TemplateResponse(
            request,
            "admin/trial_recovery_launch.html",
            {
                **self.admin_site.each_context(request),
                "title": "Перевірити й запустити recovery-кампанію",
                "opts": self.model._meta,
                "campaign": campaign,
                "form": form,
                "preview": preview,
                "excluded_rows": _trial_recovery_exclusion_rows(preview),
                "back_url": back_url,
            },
        )

    @admin_action_permission("recovery_change")
    def expand_view(self, request, object_id):
        campaign = get_object_or_404(TrialRecoveryCampaign, pk=object_id)
        back_url = reverse("admin:subscriptions_trialrecoverycampaign_change", args=[campaign.pk])
        if campaign.status not in {
            TrialRecoveryCampaign.Status.SCHEDULED,
            TrialRecoveryCampaign.Status.RUNNING,
        }:
            messages.error(request, "Розширити аудиторію можна лише для запланованої або активної кампанії.")
            return redirect(back_url)
        if campaign_uses_registered_without_card_audience(campaign):
            messages.info(request, "Кампанія вже охоплює всіх зареєстрованих користувачів без картки.")
            return redirect(back_url)

        preview = audience_preview()
        form = TrialRecoveryExpandForm(request.POST or None)
        if request.method == "POST" and form.is_valid():
            before_snapshot = campaign.audience_snapshot or {}
            try:
                result = expand_campaign_audience(
                    campaign,
                    admin_user=request.user,
                    now=timezone.now(),
                )
            except ValueError as exc:
                messages.error(request, str(exc))
            else:
                create_audit_log(
                    request=request,
                    admin_user=request.user,
                    action="trial_recovery_audience_expanded",
                    object_type="trial_recovery_campaign",
                    object_id=campaign.pk,
                    before={"audience_snapshot": before_snapshot},
                    after={"audience_snapshot": campaign.audience_snapshot, **result},
                )
                messages.success(
                    request,
                    f"Аудиторію розширено. Додано отримувачів: {result['created_recipients']}. "
                    "Повідомлення розподілено лише в дозволеному локальному вікні 10:00–19:00.",
                )
                return redirect(back_url)

        return TemplateResponse(
            request,
            "admin/trial_recovery_expand.html",
            {
                **self.admin_site.each_context(request),
                "title": "Розширити recovery на всіх зареєстрованих без картки",
                "opts": self.model._meta,
                "campaign": campaign,
                "form": form,
                "preview": preview,
                "excluded_rows": _trial_recovery_exclusion_rows(preview),
                "back_url": back_url,
            },
        )

    @admin_action_permission("recovery_change")
    def pause_view(self, request, object_id):
        campaign = get_object_or_404(TrialRecoveryCampaign, pk=object_id)
        back_url = reverse("admin:subscriptions_trialrecoverycampaign_change", args=[campaign.pk])
        if request.method != "POST":
            return TemplateResponse(
                request,
                "admin/object_action_form.html",
                {
                    **self.admin_site.each_context(request),
                    "title": "Призупинити recovery-кампанію",
                    "opts": self.model._meta,
                    "object": campaign,
                    "object_label": "Кампанія",
                    "form": TrialRecoveryPauseForm(),
                    "help_text": "Після підтвердження scheduler перестане надсилати recovery-повідомлення.",
                    "submit_label": "Призупинити кампанію",
                    "back_url": back_url,
                },
            )
        form = TrialRecoveryPauseForm(request.POST)
        if form.is_valid():
            before = campaign.status
            pause_campaign(campaign)
            create_audit_log(
                request=request,
                admin_user=request.user,
                action="trial_recovery_campaign_paused",
                object_type="trial_recovery_campaign",
                object_id=campaign.pk,
                before={"status": before},
                after={"status": TrialRecoveryCampaign.Status.PAUSED},
            )
            messages.success(request, "Recovery-кампанію призупинено.")
        return redirect(back_url)


@admin.register(TrialRecoveryRecipient, site=admin_site)
class TrialRecoveryRecipientAdmin(HiddenFromMenuAdminMixin, OperationalModelAdmin):
    list_display = ("user", "status", "source", "reason", "sent_count", "next_send_at", "contact_requested_at", "converted_at")
    list_filter = ("campaign", "status", "source", "reason", "timezone")
    search_fields = ("user__tg_user_id", "user__username", "free_text")
    readonly_fields = tuple(field.name for field in TrialRecoveryRecipient._meta.fields)
    actions = None

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(TrialRecoveryDelivery, site=admin_site)
class TrialRecoveryDeliveryAdmin(HiddenFromMenuAdminMixin, OperationalModelAdmin):
    list_display = ("recipient", "step", "status", "telegram_message_id", "sent_at", "created_at")
    list_filter = ("status", "step", "created_at")
    search_fields = ("recipient__user__tg_user_id", "recipient__user__username", "error_message")
    readonly_fields = tuple(field.name for field in TrialRecoveryDelivery._meta.fields)
    actions = None

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False
