"""BILL-001 current entitlement, with real billing resolver and synthetic SQL fixture."""
from datetime import UTC, datetime, timedelta
import unittest
from tests.test_billing_family_access import BillingFamilyAccessConn
import billing_client


class BillingAccessBatch2Tests(unittest.IsolatedAsyncioTestCase):
    async def test_expired_cached_billing_and_manual_access_is_denied(self):
        for status, source in (('trial','billing'), ('active','billing'), ('expired','billing'), ('manual','admin_manual'), ('cancelled','billing')):
            with self.subTest(status=status, source=source):
                conn = BillingFamilyAccessConn(finance_scope=None, billing_states={950001:{'subscription_status':status,'expires_at':datetime.now(UTC)-timedelta(seconds=1)}}, admin_state={'access_scope':'personal_full','access_source':source})
                state = await billing_client.get_access_state(conn, 950001)
                self.assertEqual(state['access_scope'], 'paywall')

    async def test_soft_grace_expiry_cannot_fall_back_to_projection(self):
        now = datetime.now(UTC)
        conn = BillingFamilyAccessConn(finance_scope=None, billing_states={950001:{'profile_exists':True,'subscription_status':'expired','expires_at':now-timedelta(days=1),'grace_expires_at':now+timedelta(hours=1),'last_failure_reason':'insufficient funds'}}, admin_state={'access_scope':'personal_full','access_source':'billing'})
        self.assertEqual((await billing_client.get_access_state(conn,950001))['access_scope'], 'personal_full')
        conn.billing_states[950001]['grace_expires_at'] = now-timedelta(seconds=1)
        self.assertEqual((await billing_client.get_access_state(conn,950001))['access_scope'], 'paywall')

    async def test_canonical_manual_lifetime_and_debt_only_are_preserved(self):
        for status in ('manual','lifetime'):
            conn = BillingFamilyAccessConn(finance_scope=None,billing_states={950001:{'subscription_status':status,'expires_at':None}})
            self.assertEqual((await billing_client.get_access_state(conn,950001))['access_scope'], 'personal_full')
        conn = BillingFamilyAccessConn(finance_scope=None,admin_state={'access_scope':'debt_only'})
        self.assertEqual((await billing_client.get_access_state(conn,950001))['access_scope'], 'debt_only')