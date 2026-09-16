from __future__ import annotations

from celery import shared_task

from subscriptions.billing import (
    reconcile_pending_monobank_charges,
    reconcile_monobank_refunds,
    run_due_monobank_charges,
)
from subscriptions.trial_recovery import dispatch_trial_recovery


@shared_task(name="subscriptions.run_due_monobank_charges")
def run_due_monobank_charges_task() -> dict[str, int]:
    return run_due_monobank_charges()


@shared_task(name="subscriptions.reconcile_pending_monobank_charges")
def reconcile_pending_monobank_charges_task() -> dict[str, int]:
    result = reconcile_pending_monobank_charges()
    refunds = reconcile_monobank_refunds()
    result.update({f"refund_{key}": value for key, value in refunds.items()})
    return result


@shared_task(name="subscriptions.dispatch_trial_recovery")
def dispatch_trial_recovery_task() -> dict[str, int]:
    return dispatch_trial_recovery()
