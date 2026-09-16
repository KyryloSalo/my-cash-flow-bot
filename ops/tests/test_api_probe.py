import os
from pathlib import Path
import subprocess
import sys
import unittest

CODE=Path(__file__).resolve().parents[2]

class ApiProbeTests(unittest.TestCase):
    def test_actual_asgi_retired_routes_and_missing_db_are_controlled(self):
        script='''import asyncio, httpx, api_server
async def probe():
    app=api_server.create_app()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,raise_app_exceptions=False),base_url="http://fixture") as client:
        for path in ["/admin/user/1", "/admin/user/1/transactions"]:
            assert (await client.get(path)).status_code==404
        response=await client.get("/health")
        assert response.status_code==503,response.status_code
        assert response.json()=={"status":"degraded","db":"unavailable"}
asyncio.run(probe())
'''
        safe={k:os.environ[k] for k in ['SYSTEMROOT','PATH','TEMP','TMP'] if k in os.environ}
        run=subprocess.run([sys.executable,'-c',script],cwd=CODE/'bot',env=safe,capture_output=True,text=True,timeout=30)
        self.assertEqual(run.returncode,0,run.stderr)

if __name__=='__main__': unittest.main()
