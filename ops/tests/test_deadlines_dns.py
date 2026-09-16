from pathlib import Path
import runpy
import unittest

CODE=Path(__file__).resolve().parents[2]

class DeadlineDnsTests(unittest.TestCase):
    def test_worker_budget_and_concurrency_are_explicit(self):
        path=CODE/'admin_service/gunicorn.conf.py'
        self.assertTrue(path.exists(),'worker/deadline config missing')
        c=runpy.run_path(str(path))
        self.assertEqual(c['worker_class'],'gthread')
        self.assertGreaterEqual(c['workers'],2)
        self.assertGreaterEqual(c['threads'],4)
        self.assertGreaterEqual(c['timeout'],105)
        self.assertGreaterEqual(c['graceful_timeout'],c['timeout'])

    def test_public_and_admin_proxy_use_runtime_dns(self):
        conf=(CODE/'nginx/cashflowbot.conf.template').read_text()
        self.assertNotIn('proxy_pass http://admin:8080;',conf)
        self.assertEqual(conf.count('proxy_pass http://$cashflow_admin_upstream;'),3)
        for vhost in conf.split('server {')[1:]:
            if 'proxy_pass' in vhost:
                self.assertIn('resolver 127.0.0.11',vhost)
                self.assertIn('proxy_read_timeout 130s;',vhost)

if __name__=='__main__': unittest.main()
