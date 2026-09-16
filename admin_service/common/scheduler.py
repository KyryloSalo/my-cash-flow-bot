"""Independent beat liveness, plus a default-queue lag no-op probe."""
import logging
import time
from celery.beat import PersistentScheduler
from common.readiness import BEAT_KEY, _redis_client


class HeartbeatScheduler(PersistentScheduler):
    def tick(self, *args, **kwargs):
        delay = super().tick(*args, **kwargs)
        try:
            _redis_client().set(BEAT_KEY, str(time.time()), ex=60)
        except Exception:
            logging.getLogger(__name__).warning("Beat heartbeat unavailable")
        return min(delay, 15)
