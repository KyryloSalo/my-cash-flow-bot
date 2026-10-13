from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from miniapp.models import CaptureEvent


class Command(BaseCommand):
    help = "Remove retained capture events after the configured retention window."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=30)

    def handle(self, *args, **options):
        cutoff = timezone.now() - timedelta(days=max(1, options["days"]))
        updated = CaptureEvent.objects.filter(created_at__lt=cutoff, status__in=["pending", "needs_review", "reviewed"]).update(status="expired")
        self.stdout.write(self.style.SUCCESS(f"expired {updated} unconfirmed capture records"))
