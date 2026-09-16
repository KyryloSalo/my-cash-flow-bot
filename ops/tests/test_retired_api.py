import ast
from pathlib import Path
import unittest

CODE=Path(__file__).resolve().parents[2]

class RetiredApiTests(unittest.TestCase):
    def test_no_legacy_admin_financial_readers_registered(self):
        tree=ast.parse((CODE/'bot/api_server.py').read_text(encoding='utf-8'))
        routes=[]
        for node in ast.walk(tree):
            if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)):
                for deco in node.decorator_list:
                    if isinstance(deco,ast.Call) and deco.args and isinstance(deco.args[0],ast.Constant):
                        routes.append(deco.args[0].value)
        self.assertFalse(any(str(route).startswith('/admin/') for route in routes),routes)
        self.assertNotIn('owner_tg_user_id',(CODE/'bot/api_server.py').read_text())
        self.assertIn('/internal/miniapp/export',routes)

if __name__=='__main__': unittest.main()
