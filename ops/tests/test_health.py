import ast
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest

CODE=Path(__file__).resolve().parents[2]

class HealthTests(unittest.TestCase):
    def test_admin_aggregator_returns_failed_on_independent_db_errors(self):
        path=CODE/'admin_service/common/healthcheck.py'
        tree=ast.parse(path.read_text(encoding='utf-8'))
        selected=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name in {'run_admin_healthcheck','_safe_check','_check','_worst_status'}]
        namespace={'STATUS_OK':'OK','STATUS_FAILED':'FAILED','STATUS_WARNING':'WARN','timezone':SimpleNamespace(localtime=lambda:'synthetic')}
        funcs=['_check_database','_check_redis','_check_worker','_check_telegram_api','_check_static_files','_check_pending_migrations','_check_admin_domain','_check_token_exposure','_check_broadcast_queue','_check_admin_notifications','_check_manual_message_service','_check_audit_log_table','_check_bot_events_table','_check_reset_service_dry_run','_check_admin_modules','_check_scheduler']
        for name in funcs:
            namespace[name]=lambda *a,**k:{'label':'fixture','status':'OK','detail':'fixture'}
        def unavailable(*a,**k): raise ConnectionError('Synthetic DB unavailable')
        namespace['_check_pending_migrations']=unavailable
        namespace['_check_reset_service_dry_run']=unavailable
        namespace['_check_admin_modules']=unavailable
        namespace['_check_https_and_debug']=lambda:[]
        exec(compile(ast.Module(body=selected,type_ignores=[]),str(path),'exec'),namespace)
        result=namespace['run_admin_healthcheck']()
        self.assertEqual(result['summary_status'],'FAILED')
        self.assertGreaterEqual(sum(x['status']=='FAILED' for x in result['checks']),3)

    def test_scheduler_probe_is_explicitly_imported(self):
        source=(CODE/'admin_service/config/settings.py').read_text()
        tree=ast.parse(source)
        imports=next(
            ast.literal_eval(node.value)
            for node in tree.body
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == 'CELERY_IMPORTS' for target in node.targets)
        )
        self.assertIn('common.tasks', imports)
        self.assertIn('gamification.tasks', imports)
        self.assertIn('"task": "gamification.finalize_due_profiles"', source)

    def test_gamification_flags_default_to_dark_launch(self):
        source=(CODE/'admin_service/config/settings.py').read_text()
        self.assertIn('GAMIFICATION_UI_ENABLED = env_bool("GAMIFICATION_UI_ENABLED", False)', source)
        self.assertIn(
            'GAMIFICATION_PROCESSING_ENABLED = env_bool("GAMIFICATION_PROCESSING_ENABLED", False)',
            source,
        )

    def test_machine_readiness_handles_each_unavailable_dependency(self):
        path=CODE/'admin_service/common/readiness.py'
        self.assertTrue(path.exists(),'Machine-readable readiness missing')
        spec=importlib.util.spec_from_file_location('ops_readiness',path)
        m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        def okay(): return True
        def fail(): raise ConnectionError('synthetic marker not for response')
        for name in ['database','redis','worker','beat','queue_lag','billing_lag']:
            checks={key:okay for key in ['database','redis','worker','beat','queue_lag','billing_lag']};checks[name]=fail
            status=m.readiness_status(checks=checks)
            self.assertFalse(status['ready'])
            self.assertEqual(status['checks'][name],'failed')
            self.assertNotIn('synthetic marker',str(status))
        self.assertTrue(m.readiness_status(checks={'database':okay})['ready'])

if __name__=='__main__': unittest.main()
