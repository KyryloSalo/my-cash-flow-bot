from django.urls import include, path

from common.admin_site import admin_site
from common.readiness import ready, live
from subscriptions.views import (
    monobank_cancel_autorenew,
    monobank_init_bind,
    monobank_init_recovery,
    monobank_return,
    monobank_retry_renew,
    monobank_sync_bind_status,
    monobank_webhook,
)
from miniapp.views import (
    internal_notification_event,
    internal_transaction_void_confirm,
    internal_transaction_void_draft,
    internal_transactions_recent,
)


urlpatterns = [
    path("internal/health/ready", ready, name="readiness"),
    path("internal/health/live", live, name="liveness"),
    path("app/", include(("miniapp.urls", "miniapp"), namespace="miniapp")),
    path("billing/mono/webhook", monobank_webhook, name="billing-mono-webhook"),
    path("billing/mono/return", monobank_return, name="billing-mono-return"),
    path("internal/billing/mono/init-bind", monobank_init_bind, name="billing-mono-init-bind"),
    path("internal/billing/mono/init-recovery", monobank_init_recovery, name="billing-mono-init-recovery"),
    path("internal/billing/mono/cancel-autorenew", monobank_cancel_autorenew, name="billing-mono-cancel-autorenew"),
    path("internal/billing/mono/retry-renew", monobank_retry_renew, name="billing-mono-retry-renew"),
    path("internal/billing/mono/sync-bind-status", monobank_sync_bind_status, name="billing-mono-sync-bind-status"),
    path("internal/push/event", internal_notification_event, name="push-internal-event"),
    path("internal/transactions/recent", internal_transactions_recent, name="transactions-internal-recent"),
    path("internal/transactions/void/draft", internal_transaction_void_draft, name="transactions-internal-void-draft"),
    path("internal/transactions/void/confirm", internal_transaction_void_confirm, name="transactions-internal-void-confirm"),
    path("", admin_site.urls),
]
