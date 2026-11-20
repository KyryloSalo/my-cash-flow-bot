#!/usr/bin/env python3
"""Transactional installation of Vydno-owned TLS scripts and systemd units."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess

ROOT = Path('/opt/my-cash-flow-bot')
TARGET = ROOT / 'shared/tls-ops'
SOURCE = Path(__file__).resolve().parent
SYSTEMD = Path('/etc/systemd/system')
TRANSACTION = TARGET / '.tls-install-transaction'
TIMERS = ('certbot.timer', 'vydno-tls-dry-run.timer', 'vydno-public-monitor.timer')
SCRIPTS = ('tls_ops.py', 'tls_monitor.py', 'notify_ops.py', 'notify_ops.sh')


def unit_definitions():
    return {
        'certbot.service.d/vydno.conf': f'''[Unit]
After=docker.service network-online.target
OnFailure=vydno-tls-alert@%n.service
[Service]
ExecStart=
ExecStart=/usr/bin/python3 {TARGET}/tls_ops.py renew
TimeoutStartSec=1900
''',
        'vydno-tls-dry-run.service': f'''[Unit]
Description=Verify Vydno certificate renewal with ACME staging
After=docker.service network-online.target
OnFailure=vydno-tls-alert@%n.service
[Service]
Type=oneshot
ExecStart=/usr/bin/python3 {TARGET}/tls_ops.py dry-run
TimeoutStartSec=1900
''',
        'vydno-tls-dry-run.timer': '''[Unit]
Description=Weekly Vydno certificate renewal rehearsal
[Timer]
OnCalendar=Sun *-*-* 03:00:00
RandomizedDelaySec=1h
Persistent=true
[Install]
WantedBy=timers.target
''',
        'vydno-tls-alert@.service': f'''[Unit]
Description=Notify administrators about failed Vydno TLS operation %i
After=docker.service network-online.target
StartLimitIntervalSec=0
[Service]
Type=oneshot
ExecStart=/bin/bash {TARGET}/notify_ops.sh "Vydno TLS operation %i FAILED. Inspect its systemd journal."
Restart=on-failure
RestartSec=300
TimeoutStartSec=90
''',
        'vydno-public-monitor.service': f'''[Unit]
Description=Check public Vydno HTTPS and certificate expiry
After=docker.service network-online.target
OnFailure=vydno-tls-alert@%n.service
[Service]
Type=oneshot
ExecStart=/usr/bin/python3 {TARGET}/tls_monitor.py --emit --state {TARGET}/monitor-state.json --notify-command-json '["/bin/bash","{TARGET}/notify_ops.sh"]'
TimeoutStartSec=240
''',
        'vydno-public-monitor.timer': '''[Unit]
Description=Check Vydno public HTTPS every five minutes
[Timer]
OnBootSec=2min
OnUnitActiveSec=5min
Persistent=true
[Install]
WantedBy=timers.target
''',
    }


def control(*args, check=True):
    return subprocess.run(['systemctl', *args], check=check, text=True, capture_output=True)


def systemctl_state(unit):
    enabled = control('is-enabled', unit, check=False).stdout.strip()
    active = control('is-active', unit, check=False).stdout.strip()
    if enabled not in ('enabled', 'enabled-runtime', 'disabled', 'not-found'):
        raise RuntimeError(f'Unsupported timer enablement state for {unit}: {enabled}')
    if active not in ('active', 'inactive', 'failed', 'unknown'):
        raise RuntimeError(f'Unsupported timer activity state for {unit}: {active}')
    return enabled, active


def paths():
    return [TARGET / n for n in SCRIPTS] + [SYSTEMD / n for n in unit_definitions()]


def snapshot():
    if TRANSACTION.exists():
        raise RuntimeError('Unresolved TLS transaction; rollback or commit before reinstalling')
    TRANSACTION.mkdir(parents=True, mode=0o700)
    try:
        records = []
        for index, path in enumerate(paths()):
            if path.is_symlink():
                raise RuntimeError(f'Refusing symlink at owned TLS path: {path}')
            backup = TRANSACTION / 'files' / str(index)
            exists = path.is_file()
            if exists:
                backup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, backup)
            records.append({'path': str(path), 'backup': str(backup), 'exists': exists})
        state = {'files': records, 'timers': {u: systemctl_state(u) for u in TIMERS}}
        (TRANSACTION / 'state.json').write_text(json.dumps(state), encoding='utf-8')
    except BaseException:
        shutil.rmtree(TRANSACTION)
        raise


def stop_timers():
    for unit in TIMERS:
        if control('is-active', unit, check=False).stdout.strip() == 'active':
            control('stop', unit)
    for unit in ('certbot.service', 'vydno-tls-dry-run.service', 'vydno-public-monitor.service'):
        if control('is-active', unit, check=False).stdout.strip() in ('active', 'activating', 'deactivating'):
            raise RuntimeError(f'Wait for active TLS operation before installing: {unit}')


def restore():
    if not TRANSACTION.exists():
        return
    state = json.loads((TRANSACTION / 'state.json').read_text(encoding='utf-8'))
    for unit in TIMERS:
        if control('is-active', unit, check=False).stdout.strip() == 'active':
            control('stop', unit)
        if control('is-enabled', unit, check=False).stdout.strip() in ('enabled', 'enabled-runtime'):
            control('disable', unit)
    for record in state['files']:
        path = Path(record['path'])
        if record['exists']:
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(record['backup'], path)
        else:
            path.unlink(missing_ok=True)
    control('daemon-reload')
    for unit, (enabled, active) in state['timers'].items():
        if enabled in ('enabled', 'enabled-runtime'):
            control('enable', *(['--runtime'] if enabled == 'enabled-runtime' else []), unit)
        if active == 'active':
            control('start', unit)
    shutil.rmtree(TRANSACTION)


def install():
    if hasattr(os, 'geteuid') and os.geteuid() != 0:
        raise RuntimeError('TLS installation requires root')
    sources = {name: (SOURCE / name).read_bytes() for name in SCRIPTS}
    definitions = unit_definitions()
    snapshot()
    try:
        stop_timers()
        for path in (ROOT / 'shared', ROOT / 'shared/acme-webroot', TARGET):
            path.mkdir(parents=True, exist_ok=True, mode=0o755)
            path.chmod(0o755)
        for name, content in sources.items():
            temporary = TARGET / (name + '.new')
            temporary.write_bytes(content)
            temporary.chmod(0o755)
            temporary.replace(TARGET / name)
        for name, content in definitions.items():
            path = SYSTEMD / name
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(path.name + '.new')
            temporary.write_text(content, encoding='utf-8')
            temporary.chmod(0o644)
            temporary.replace(path)
        control('daemon-reload')
        control('enable', '--now', *TIMERS)
    except BaseException as install_error:
        try:
            restore()
        except BaseException as rollback_error:
            raise RuntimeError(f'TLS installation AND rollback failed: {type(rollback_error).__name__}; transaction retained') from install_error
        raise
    print('TLS controls installed; rollback snapshot retained pending production acceptance.')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', nargs='?', default='install', choices=('install', 'rollback', 'commit'))
    args = parser.parse_args()
    if args.command == 'install':
        install()
    elif args.command == 'rollback':
        restore()
    elif TRANSACTION.exists():
        shutil.rmtree(TRANSACTION)


if __name__ == '__main__':
    main()
