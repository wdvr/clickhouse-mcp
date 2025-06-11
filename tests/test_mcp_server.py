import unittest
from clickhouse_mcp.mcp_server import mcp

class TestMcpServerApp(unittest.TestCase):
    def test_app_has_mcp_route(self):
        app = mcp.streamable_http_app()
        paths = [route.path for route in app.routes]
        self.assertIn('/mcp', paths)

if __name__ == '__main__':
    unittest.main()
