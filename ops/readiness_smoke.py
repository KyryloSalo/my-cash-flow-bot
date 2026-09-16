"""Read-only public route gates. Never supply internal tokens or make payments."""
import argparse
import urllib.request
import urllib.error


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs): return None


def check_url(url, expected):
    opener=urllib.request.build_opener(NoRedirect)
    try:
        with opener.open(url,timeout=10) as response: status=response.status
    except urllib.error.HTTPError as exc: status=exc.code
    if status not in expected:
        raise RuntimeError(f'Unexpected route status {status} for {url.split("?", 1)[0]}')
    print(url.split('?',1)[0],status)


def main():
    p=argparse.ArgumentParser();p.add_argument('--domain',default='vydno.capital');p.add_argument('--admin',default='admin.vydno.capital');p.add_argument('--legacy',default='cashflowbot.mygpttest.space');args=p.parse_args()
    base='https://'+args.domain
    for route,status in [('/',{200}),('/app/',{200}),('/app/api/profile',{401}),('/billing/mono/webhook',{405})]: check_url(base+route,status)
    for host in [args.domain,'www.'+args.domain,args.admin,args.legacy]:
        for scheme in ['http','https']:
            for path in ['/internal','/internal/push/event','/internal/billing/mono/init-bind','/internal/transactions/recent','/internal/health/ready']:
                check_url(scheme+'://'+host+path,{403,404})
    # A production IP allowlist may intentionally deny this unauthenticated probe.
    check_url('https://'+args.admin+'/',{200,302,401,403})


if __name__=='__main__':main()
