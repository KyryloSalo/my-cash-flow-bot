#!/usr/bin/env python3
"""Read-only public HTTPS/TLS monitor (stdlib only)."""
import argparse, hashlib, json, os, socket, ssl, subprocess, sys, tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

DEFAULT_DOMAINS = ["vydno.capital", "admin.vydno.capital", "cashflowbot.mygpttest.space", "www.vydno.capital"]
APP_DOMAINS = {"vydno.capital", "admin.vydno.capital"}
THRESHOLDS = (21, 14, 7)

def _expiry(host, port=443, timeout=10):
    ctx = ssl.create_default_context()
    with socket.create_connection((host, port), timeout=timeout) as raw:
        with ctx.wrap_socket(raw, server_hostname=host) as sock:
            cert = sock.getpeercert()
    try: return datetime.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z").replace(tzinfo=timezone.utc)
    except (KeyError, ValueError) as exc: raise ValueError("certificate has no valid notAfter") from exc

def _get(url, timeout=10):
    host = url.split("/", 3)[2]; req = Request(url, headers={"User-Agent": "mcf-public-monitor/1"})
    try:
        with urlopen(req, timeout=timeout, context=ssl.create_default_context()) as response:
            return response.status, response.read(8192), _expiry(host, timeout=timeout)
    except HTTPError as exc:
        return exc.code, exc.read(8192), _expiry(host, timeout=timeout)

def _json_body(body):
    try: return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, AttributeError): return None

def check_domain(domain, probe=_get, now=None, timeout=10):
    now = now or datetime.now(timezone.utc); reasons, details, expiry = [], {}, None
    try:
        if domain in APP_DOMAINS:
            status, body, expiry = probe(f"https://{domain}/app/", timeout=timeout); details["app_status"] = status
            if status != 200: reasons.append(f"app_status_{status}")
        else: expiry = _expiry(domain, timeout=timeout)
    except Exception as exc: reasons.append("https_unreachable"); details["https_error"] = type(exc).__name__
    if domain in APP_DOMAINS:
        try:
            status, body, cert_expiry = probe(f"https://{domain}/app/api/bootstrap", timeout=timeout); expiry = expiry or cert_expiry; details["dashboard_status"] = status
            payload = _json_body(body)
            error = payload.get("error") if isinstance(payload, dict) else None
            if status != 401 or not isinstance(error, dict) or error.get("code") != "session_required": reasons.append("dashboard_contract")
        except Exception as exc: reasons.append("dashboard_unreachable"); details["dashboard_error"] = type(exc).__name__
    if expiry is None:
        try: expiry = _expiry(domain, timeout=timeout)
        except Exception as exc: reasons.append("tls_certificate_unavailable"); details["tls_error"] = type(exc).__name__
    if expiry is not None:
        days = (expiry - now).total_seconds() / 86400; details.update(certificate_expires_at=expiry.isoformat(), certificate_days=round(days, 2))
        if days <= 7: reasons.append("certificate_expiry_7d")
        elif days <= 14: reasons.append("certificate_expiry_14d")
        elif days <= 21: reasons.append("certificate_expiry_21d")
    return {"domain": domain, "status": "alert" if reasons else "ok", "reasons": reasons, "details": details}

def build_report(domains=None, probe=_get, now=None):
    checked = now or datetime.now(timezone.utc); items = [check_domain(d, probe=probe, now=checked) for d in (domains or DEFAULT_DOMAINS)]
    return {"status": "alert" if any(x["status"] == "alert" for x in items) else "ok", "domains": items, "checked_at": checked.isoformat()}

def _load(path):
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8")); return data if isinstance(data, dict) and isinstance(data.get("active", {}), dict) else {"active": {}}
    except (FileNotFoundError, json.JSONDecodeError, OSError): return {"active": {}}

def _save(path, data):
    target = Path(path); target.parent.mkdir(parents=True, exist_ok=True); fd, tmp = tempfile.mkstemp(prefix=target.name + ".", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, sort_keys=True, separators=(",", ":")); handle.flush(); os.fsync(handle.fileno())
        os.replace(tmp, target)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)

def _fingerprint(alert): return alert.get("fingerprint") or hashlib.sha256(alert.get("message", "").encode()).hexdigest()
def should_emit(path, alert): return _load(path).get("active", {}).get(alert["key"], {}).get("fingerprint") != _fingerprint(alert)
def record_alert(path, alert):
    data = _load(path); data.setdefault("active", {})[alert["key"]] = {"fingerprint": _fingerprint(alert), "alert": alert}; _save(path, data)
def record_recovery(path, key):
    data = _load(path); data.setdefault("active", {}).pop(key, None); _save(path, data)
def notify(argv, message):
    if not argv: raise ValueError("notifier command is empty")
    result = subprocess.run(argv, input=message, text=True, capture_output=True, timeout=30)
    if result.returncode: raise RuntimeError(f"notifier failed with exit code {result.returncode}")

def main(argv=None):
    ap = argparse.ArgumentParser(); ap.add_argument("--domains", default=",".join(DEFAULT_DOMAINS)); ap.add_argument("--state", default="/var/lib/my-cash-flow-bot/tls-monitor.json"); ap.add_argument("--emit", action="store_true"); ap.add_argument("--json", action="store_true"); ap.add_argument("--check", action="store_true"); ap.add_argument("--notify-command-json")
    ap.add_argument("--once", action="store_true", help="Run one check (the default).")
    args = ap.parse_args(argv)
    try: notify_argv = json.loads(args.notify_command_json) if args.notify_command_json else None
    except json.JSONDecodeError as exc: ap.error(f"--notify-command-json must be a JSON string array: {exc}")
    if notify_argv is not None and (not isinstance(notify_argv, list) or not notify_argv or not all(isinstance(x, str) and x for x in notify_argv)): ap.error("--notify-command-json must be a non-empty JSON string array")
    report = build_report([d for d in args.domains.split(",") if d]); emitted = []
    for item in report["domains"]:
        key = "tls:" + item["domain"]
        if item["status"] == "alert":
            alert = {"key": key, "severity": "critical", "message": f"TLS/public availability alert for {item['domain']}: {', '.join(item['reasons'])}", "fingerprint": "|".join(item["reasons"])}
            if args.emit and should_emit(args.state, alert):
                try:
                    if notify_argv: notify(notify_argv, alert["message"])
                except Exception as exc: alert["notify_error"] = type(exc).__name__; emitted.append(alert); continue
                record_alert(args.state, alert); emitted.append(alert)
        elif args.emit and _load(args.state).get("active", {}).get(key):
            recovery = f"TLS/public availability recovered for {item['domain']}"
            try:
                if notify_argv: notify(notify_argv, recovery)
            except Exception as exc: emitted.append({"key": key, "message": recovery, "notify_error": type(exc).__name__}); continue
            record_recovery(args.state, key); emitted.append({"key": key, "message": recovery, "recovery": True})
    print(json.dumps({"alerts": emitted, "report": report} if args.emit else report, sort_keys=True)); return 1 if args.check and report["status"] != "ok" else 0

if __name__ == "__main__": sys.exit(main())