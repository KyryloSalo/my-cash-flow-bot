from django.urls import path
from subscriptions.views import monobank_retry_renew
from common.admin_site import admin_site
import subscriptions.admin
urlpatterns = [path('internal/billing/mono/retry-renew', monobank_retry_renew),path('admin/',admin_site.urls)]
