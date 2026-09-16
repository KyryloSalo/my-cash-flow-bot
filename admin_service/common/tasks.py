"""Monitoring only: does not send messages or touch financial tables."""
import time
from celery import shared_task
from common.readiness import QUEUE_KEY, _redis_client


@shared_task(name="common.scheduler_probe", ignore_result=True)
def scheduler_probe():
    _redis_client().set(QUEUE_KEY, str(time.time()), ex=120)
