"""Run normal Django migrations/tests against the real shared PostgreSQL schema.

Unmanaged means "Django does not create this table", not "test rows survive
TransactionTestCase.flush". Add only the explicit runtime inventory to test
introspection; never toggle model ownership or disable migrations. This patch
exists only for run_tests and is restored even if database setup/tests fail.
"""
from unittest.mock import patch

from django.core.exceptions import ImproperlyConfigured
from django.db import connections
from django.db.backends.base.introspection import BaseDatabaseIntrospection
from django.test.runner import DiscoverRunner

from common.runtime_schema import CORE_TABLES


class RuntimeSchemaTestRunner(DiscoverRunner):
    def run_tests(self, test_labels, **kwargs):
        if self.parallel > 1:
            raise ImproperlyConfigured("RuntimeSchemaTestRunner currently requires --parallel=1")
        for connection in connections.all():
            name = connection.settings_dict["NAME"]
            test_name = connection.settings_dict.get("TEST", {}).get("NAME")
            if test_name and test_name == name:
                raise ImproperlyConfigured("TEST.NAME must differ from the application database")

        original_tables = BaseDatabaseIntrospection.django_table_names
        original_sequences = BaseDatabaseIntrospection.sequence_list

        def table_names(introspection, only_existing=False, include_views=True):
            tables = set(original_tables(introspection, only_existing, include_views))
            if introspection.connection.vendor == "postgresql":
                runtime = set(CORE_TABLES)
                if only_existing:
                    runtime &= set(introspection.table_names(include_views=include_views))
                tables.update(runtime)
            return sorted(tables)

        def sequence_list(introspection):
            sequences = list(original_sequences(introspection))
            if introspection.connection.vendor == "postgresql":
                existing = set(introspection.table_names())
                seen = {(item["table"], item["column"]) for item in sequences}
                with introspection.connection.cursor() as cursor:
                    for table in sorted(set(CORE_TABLES) & existing):
                        for item in introspection.get_sequences(cursor, table):
                            key = (table, item["column"])
                            if key not in seen:
                                sequences.append({**item, "table": table})
                                seen.add(key)
            return sequences

        with patch.object(BaseDatabaseIntrospection, "django_table_names", table_names), patch.object(
            BaseDatabaseIntrospection, "sequence_list", sequence_list
        ):
            return super().run_tests(test_labels, **kwargs)
