#!/usr/bin/env python
"""Run the approved existing-partner-links scenario in disposable PostgreSQL/Chromium."""
import os
from uuid import uuid4
from partner_payments_acceptance import main
from partner_cabinet_browser import browser_acceptance

if __name__ == "__main__":
    # Ephemeral synthetic login, never printed or persisted.
    os.environ["MCF_ACCEPTANCE_PASSWORD"] = uuid4().hex
    main(seed_module="ops.partner_cabinet_acceptance_support", browser_scenario=browser_acceptance,
         default_tests=["miniapp.tests_partner_cabinet_period", "miniapp.tests_partner_cabinet_admin", "miniapp.tests_partner_links", "dashboard.tests_partner_payments", "miniapp.tests_partner_attribution", "common.tests"])
