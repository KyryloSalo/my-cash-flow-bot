from __future__ import annotations

from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from django.utils.dateparse import parse_datetime


class Command(BaseCommand):
    help = "Purge privacy-bounded funnel telemetry before an explicit cutoff. Dry-run by default."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--before")
        parser.add_argument("--older-than-days", type=int)
        parser.add_argument("--apply", action="store_true")
        parser.add_argument("--batch-size", type=int, default=1000)

    def handle(self, *args, **options):
        before = str(options.get("before") or "").strip()
        older_than_days = options.get("older_than_days")
        if bool(before) + (older_than_days is not None) != 1:
            raise CommandError("Provide exactly one of --before or --older-than-days.")
        if older_than_days is not None:
            if older_than_days <= 0:
                raise CommandError("--older-than-days must be positive.")
            cutoff = timezone.now() - timedelta(days=older_than_days)
        else:
            cutoff = parse_datetime(before)
            if cutoff is None:
                raise CommandError("--before must be an ISO-8601 datetime.")
            if timezone.is_naive(cutoff):
                cutoff = timezone.make_aware(cutoff, timezone.get_current_timezone())
        batch_size = int(options.get("batch_size") or 0)
        if batch_size <= 0 or batch_size > 10000:
            raise CommandError("--batch-size must be between 1 and 10000.")

        from miniapp.retention import purge_funnel_telemetry

        result = purge_funnel_telemetry(
            cutoff=cutoff,
            apply=bool(options.get("apply")),
            batch_size=batch_size,
        )
        mode = "APPLY" if options.get("apply") else "DRY-RUN"
        self.stdout.write(
            f"{mode} cutoff={cutoff.isoformat()} "
            f"events={result['events']} sessions={result['sessions']} batches={result['batches']}"
        )
