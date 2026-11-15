from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import io
import json
import os
import threading
import unittest
from unittest.mock import patch

from bank.release import Trino


class TrinoRoutingTest(unittest.TestCase):
    def test_next_uri_cannot_downgrade_https_or_change_port(self):
        for route in ['http://trino.example:8443/v1/next','https://trino.example:8444/v1/next',
                      'https://user:password@trino.example:8443/v1/next']:
            with self.subTest(route=route),patch.dict(os.environ,{'TRINO_URL':'https://trino.example:8443'}):
                client = Trino()
                payload = io.BytesIO(json.dumps({'nextUri':route}).encode())
                with patch.object(client,'_open',return_value=payload) as fetch:
                    with self.assertRaises(ValueError):
                        list(client.pages('SELECT 1'))
                    self.assertEqual(fetch.call_count,1)

    def test_cross_origin_redirect_is_blocked_before_target_is_contacted(self):
        received = []
        class Target(BaseHTTPRequestHandler):
            def log_message(self,*args):
                pass
            def do_GET(self):
                received.append(True)
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{}')
        target = ThreadingHTTPServer(('127.0.0.1',0),Target)
        class Redirect(BaseHTTPRequestHandler):
            def log_message(self,*args):
                pass
            def do_POST(self):
                self.send_response(302)
                self.send_header('Location',f'http://127.0.0.1:{target.server_port}/capture')
                self.end_headers()
        source = ThreadingHTTPServer(('127.0.0.1',0),Redirect)
        threads = [threading.Thread(target=server.serve_forever,daemon=True) for server in (target,source)]
        for thread in threads:
            thread.start()
        try:
            with patch.dict(os.environ,{'TRINO_URL':f'http://127.0.0.1:{source.server_port}'}):
                with self.assertRaisesRegex(ValueError,'redirect origin'):
                    list(Trino().pages('SELECT 1'))
            self.assertEqual(received,[])
        finally:
            for server in (source,target):
                server.shutdown()
                server.server_close()
            for thread in threads:
                thread.join(2)
