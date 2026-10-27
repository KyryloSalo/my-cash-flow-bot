"""Synthetic-only Django runner: patch provider in server process, then serve."""
import os, sys
sys.path.insert(0, os.getcwd())
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
import django
django.setup()
from miniapp import views

def synthetic(*, image_bytes, mime_type, default_currency, catalog):
    source = os.environ.get("RECOGNITION_ACCEPTANCE_SOURCE", "statement")
    if source == "statement":
        return {"mode":"statement_expenses", "source":"statement_transactions", "currency":"UAH", "currency_explicit":True,
                "statement_items":[
                    {"id":"valid-1","amount":"125.00","currency":None,"comment":"Synthetic coffee","category_slug":"food","transaction_date":None},
                    {"id":"valid-2","amount":"45.00","currency":"UAH","comment":"Synthetic lunch","category_slug":"food","transaction_date":"2026-10-20"}],
                "statement_review_rows":[{"row":3,"amount":"not-a-number","currency":"UAH","comment":"Excluded review row","transaction_date":None}],
                "skipped_items_count":1}
    return {"mode":"single_tx","type":"expense","intent":"expense","is_candidate_tx":True,
            "amount":"125.00","currency":"UAH","currency_explicit":True,
            "category_slug":"food","comment":"Synthetic coffee","confidence":0.95,
            "transaction_date":None,"source":source,"category_hint":"food",
            "needs_review_bool":True,"field_findings":["transaction_date_unknown"]}
views.request_ai_image_parse = synthetic
from django.core.management import execute_from_command_line
execute_from_command_line([sys.argv[0], "runserver", sys.argv[1], "--noreload"])
