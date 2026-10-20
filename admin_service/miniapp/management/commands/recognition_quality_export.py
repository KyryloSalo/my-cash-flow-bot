import json
import os
import uuid
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from miniapp.recognition_quality import export_weekly
from miniapp.recognition_storage import MAX_STORAGE_BYTES, _disk_usage, _locked_config, _private_root

class Command(BaseCommand):
    help = 'Write a bounded, private analyst export; stdout contains operational metadata only.'

    def add_arguments(self, parser):
        parser.add_argument('--requester-id', type=int, required=True)
        parser.add_argument('--limit', type=int, default=100)
        parser.add_argument('--metadata-only', action='store_true')

    def handle(self, *args, **options):
        if not 1 <= options['limit'] <= 1000:
            raise CommandError('limit must be between 1 and 1000')
        destination = None
        try:
            with transaction.atomic():
                cfg = _locked_config()
                report = export_weekly(requester_id=options['requester_id'],
                                       export_token=getattr(settings, 'RECOGNITION_QUALITY_EXPORT_TOKEN', None),
                                       limit=options['limit'], include_raw=not options['metadata_only'])
                data = json.dumps(report, ensure_ascii=False).encode('utf-8')
                root = _private_root()
                quota = min(cfg.max_total_bytes, MAX_STORAGE_BYTES) if cfg else MAX_STORAGE_BYTES
                if _disk_usage(root) + len(data) > quota:
                    raise CommandError('Private export would exceed recognition storage quota')
                destination = root / (uuid.uuid4().hex + '.export.json')
                flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0)
                with os.fdopen(os.open(destination, flags, 0o600), 'wb') as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
        except (PermissionError, OSError, ValueError):
            if destination:
                destination.unlink(missing_ok=True)
            raise CommandError('Protected export failed; check operator, private root and bounded page size') from None
        self.stdout.write(f'EXPORTED file={destination.name} count={len(report["items"])} has_more={report["has_more"]}')
