from django.urls import path

from . import views

app_name = "gamification"

urlpatterns = [
    path("overview", views.overview, name="overview"),
    path("preferences", views.preferences, name="preferences"),
    path("no-expenses", views.no_expenses, name="no-expenses"),
    path("pins", views.pins, name="pins"),
    path("notifications/claim", views.notification_claim, name="notification-claim"),
    path("notifications/ack", views.notification_ack, name="notification-ack"),
]
