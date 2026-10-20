"""Delete private files only after the associated database deletion commits."""
from django.db import transaction
from django.db.models.signals import post_delete
from django.dispatch import receiver

from miniapp.models import RecognitionCapture
from miniapp.recognition_storage import _private_root, _unlink_media, invalidate_private_exports


@receiver(post_delete, sender=RecognitionCapture, dispatch_uid='recognition-private-media-delete')
def delete_private_media(sender, instance, using, **kwargs):
    transaction.on_commit(invalidate_private_exports, using=using)
    if not instance.raw_media:
        return
    try:
        root = _private_root()
    except OSError:
        return  # Orphan remains charged to quota; cleanup can retry after config repair.
    key = instance.raw_media
    transaction.on_commit(lambda: _unlink_media(root, key), using=using)
