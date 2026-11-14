import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from contextlib import ExitStack, contextmanager

OPS = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('installer', OPS / 'install_tls_ops.py')
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


class TLSInstallTransactionTests(unittest.TestCase):
    @contextmanager
    def host(self, fail_enable=False):
        with tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
            root = Path(tmp) / 'root'
            target = root / 'shared/tls-ops'
            systemd = Path(tmp) / 'systemd'
            target.mkdir(parents=True)
            systemd.mkdir()
            states = {u: ['disabled', 'inactive'] for u in installer.TIMERS}
            states['certbot.timer'] = ['enabled', 'active']
            initial = {u: list(v) for u, v in states.items()}
            calls = []
            failed = False
            def control(*args, check=True):
                nonlocal failed
                calls.append(args)
                verb = args[0]
                if verb in ('is-enabled', 'is-active'):
                    values = states.get(args[-1], ['not-found', 'inactive'])
                    return subprocess.CompletedProcess(args, 0, stdout=values[0 if verb == 'is-enabled' else 1])
                if verb == 'enable' and '--now' in args and fail_enable and not failed:
                    failed = True
                    raise RuntimeError('injected enable failure')
                for unit in (x for x in args[1:] if x.endswith('.timer')):
                    if verb == 'stop': states[unit][1] = 'inactive'
                    elif verb == 'disable': states[unit][0] = 'disabled'
                    elif verb == 'start': states[unit][1] = 'active'
                    elif verb == 'enable':
                        states[unit][0] = 'enabled'
                        if '--now' in args: states[unit][1] = 'active'
                return subprocess.CompletedProcess(args, 0, stdout='')
            for name, value in {'ROOT': root, 'TARGET': target, 'SYSTEMD': systemd, 'SOURCE': OPS,
                                'TRANSACTION': target / '.tls-install-transaction', 'control': control}.items():
                stack.enter_context(patch.object(installer, name, value))
            stack.enter_context(patch.object(installer.os, 'geteuid', create=True, return_value=0))
            yield target, systemd, states, initial, calls

    def test_failed_install_restores_owned_files_and_timer_states(self):
        with self.host(fail_enable=True) as (target, systemd, states, initial, calls):
            (target / 'tls_ops.py').write_text('old', encoding='utf-8')
            (systemd / 'vydno-tls-dry-run.timer').write_text('old-unit', encoding='utf-8')
            with self.assertRaisesRegex(RuntimeError, 'injected enable'):
                installer.install()
            self.assertEqual((target / 'tls_ops.py').read_text(encoding='utf-8'), 'old')
            self.assertEqual((systemd / 'vydno-tls-dry-run.timer').read_text(encoding='utf-8'), 'old-unit')
            self.assertFalse((target / 'tls_monitor.py').exists())
            self.assertEqual(states, initial)
            self.assertFalse(installer.TRANSACTION.exists())
            self.assertLess(max(i for i, x in enumerate(calls) if x == ('daemon-reload',)),
                            calls.index(('start', 'certbot.timer')))

    def test_success_can_be_rolled_back_after_deploy_acceptance_fails(self):
        with self.host() as (target, systemd, states, initial, calls):
            (target / 'tls_ops.py').write_text('previous')
            installer.install()
            self.assertTrue(installer.TRANSACTION.exists())
            self.assertNotEqual((target / 'tls_ops.py').read_text(), 'previous')
            installer.restore()
            self.assertEqual((target / 'tls_ops.py').read_text(), 'previous')
            self.assertEqual(states, initial)
            self.assertFalse((systemd / 'certbot.service.d/vydno.conf').exists())
            installer.restore()  # rollback after an already-reverted installer is safe

    def test_unresolved_transaction_cannot_be_overwritten(self):
        with self.host():
            installer.snapshot()
            original = (installer.TRANSACTION / 'state.json').read_bytes()
            with self.assertRaisesRegex(RuntimeError, 'Unresolved'):
                installer.snapshot()
            self.assertEqual((installer.TRANSACTION / 'state.json').read_bytes(), original)

    def test_unit_commands_preserve_real_notification_delivery(self):
        units = installer.unit_definitions()
        monitor = units['vydno-public-monitor.service']
        self.assertIn('--emit --state', monitor)
        self.assertIn('--notify-command-json', monitor)
        self.assertIn('notify_ops.sh "Vydno TLS operation %i FAILED.', units['vydno-tls-alert@.service'])

    def test_deploy_trap_invokes_tls_rollback_before_restoring_app(self):
        deploy = (OPS.parent / 'deploy_v0_on_vps.sh').read_text(encoding='utf-8')
        self.assertIn('tls_installer rollback', deploy)
        self.assertLess(deploy.index('tls_installer rollback'), deploy.index('compose_at "${ACTIVE_PROJECT:-$OLD_PROJECT}" stop'))
        self.assertLess(deploy.index('tls_acceptance.py" --production'), deploy.index('tls_installer commit'))


if __name__ == '__main__':
    unittest.main()
