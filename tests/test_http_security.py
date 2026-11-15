from datetime import datetime, timezone
from email.message import Message
from http.server import ThreadingHTTPServer
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from bank.http_security import TokenAuth
from bank.serving import handler as serving_handler
from bank.exporter import handler as exporter_handler

TOKEN = 'synthetic_test_token_' + 'a'*32
OTHER = 'synthetic_test_token_' + 'b'*32


def headers(token=None,host='127.0.0.1:8090',origin=None):
    result = Message()
    result['Host'] = host
    if token is not None:
        result['Authorization'] = 'Bearer '+token
    if origin is not None:
        result['Origin'] = origin
    return result


class HttpSecurityTest(unittest.TestCase):
    def server(self,handler):
        server = ThreadingHTTPServer(('127.0.0.1',0),handler)
        worker = threading.Thread(target=server.serve_forever,daemon=True)
        worker.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f'http://127.0.0.1:{server.server_port}'

    def request(self,url,expected=200,token=None,extra=None):
        values = dict(extra or {})
        if token is not None:
            values['Authorization'] = 'Bearer '+token
        try:
            response = urlopen(Request(url,headers=values))
        except HTTPError as response:
            status,body,metadata = response.code,response.read(),response.headers
        else:
            with response:
                status,body,metadata = response.status,response.read(),response.headers
        self.assertEqual(status,expected)
        self.assertNotIn(TOKEN.encode(),body)
        self.assertEqual(metadata.get('Cache-Control'),'no-store')
        self.assertIsNone(metadata.get('Access-Control-Allow-Origin'))
        self.assertIsNone(metadata.get('Set-Cookie'))
        return body,metadata

    def test_environment_configuration_requires_valid_secret_and_expiry(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'token'
            path.write_text(TOKEN+'\r\n')
            with patch.dict(os.environ,{'BANK_HTTP_TOKEN_FILE':str(path),
                'BANK_HTTP_ALLOWED_HOSTS':'bank.example,localhost'},clear=True):
                auth = TokenAuth.from_env(require=True)
                self.assertEqual(auth.request_status(headers(TOKEN,host='bank.example',origin='https://bank.example')),200)
            with patch.dict(os.environ,{'BANK_HTTP_TOKEN':TOKEN,
                'BANK_HTTP_TOKEN_EXPIRES_AT':'2000-01-01T00:00:00Z'},clear=True),self.assertRaises(ValueError):
                TokenAuth.from_env(require=True)
            with patch.dict(os.environ,{'BANK_HTTP_TOKEN':TOKEN,
                'BANK_HTTP_TOKEN_FILE':str(path)},clear=True),self.assertRaises(ValueError):
                TokenAuth.from_env()

    def test_bearer_validation_rotation_and_expiry(self):
        now = datetime(2026,1,1,tzinfo=timezone.utc).timestamp()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'access-token'
            path.write_text(TOKEN+'\n')
            auth = TokenAuth(token_file=path,expires_at='2026-01-01T00:01:00Z',clock=lambda:now)
            self.assertEqual(auth.request_status(headers(TOKEN)),200)
            self.assertEqual(auth.request_status(headers(OTHER)),401)
            path.write_text(OTHER+'\n')
            self.assertEqual(auth.request_status(headers(TOKEN)),401)
            self.assertEqual(auth.request_status(headers(OTHER)),200)
            path.unlink()
            self.assertEqual(auth.request_status(headers(OTHER)),503)
            path.write_text(OTHER)
            now += 60
            self.assertEqual(auth.request_status(headers(OTHER)),401)

    def test_fail_closed_configuration_and_duplicate_credentials(self):
        for token in ['short','a'*513,TOKEN+'\nextra',' '+TOKEN,'é'*40]:
            with self.subTest(token_length=len(token)),self.assertRaises(ValueError):
                TokenAuth(token=token)
        with tempfile.TemporaryDirectory() as tmp,self.assertRaises(ValueError):
            TokenAuth(token_file=Path(tmp)/'missing')
        with patch.dict(os.environ,{},clear=True),self.assertRaises(ValueError):
            TokenAuth.from_env(require=True)
        auth = TokenAuth(token=TOKEN)
        values = headers(TOKEN)
        values['Authorization'] = 'Bearer '+TOKEN
        self.assertEqual(auth.request_status(values),401)
        values = headers(TOKEN)
        values['Host'] = '127.0.0.1:8090'
        self.assertEqual(auth.request_status(values),400)

    def test_host_origin_and_cross_site_guards_apply_without_auth(self):
        auth = TokenAuth()
        self.assertEqual(auth.request_status(headers()),200)
        self.assertEqual(auth.request_status(headers(host='rebinding.example:8090')),403)
        self.assertEqual(auth.request_status(headers(origin='http://evil.example')),403)
        self.assertEqual(auth.request_status(headers(origin='null')),403)
        self.assertEqual(auth.request_status(headers(origin='http://127.0.0.1:8090')),200)
        self.assertEqual(auth.request_status(headers(origin='http://127.0.0.1:8091')),403)
        values = headers()
        values['Sec-Fetch-Site'] = 'cross-site'
        self.assertEqual(auth.request_status(values),403)
        self.assertEqual(auth.request_status(values,protected=False),200)
        configured = TokenAuth(allowed_hosts=['bank.example'])
        self.assertEqual(configured.request_status(headers(host='bank.example',origin='https://bank.example')),200)
        with self.assertRaises(ValueError):
            TokenAuth(allowed_hosts=['*'])

    def test_serving_http_auth_liveness_and_no_token_in_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = self.server(serving_handler(Path(tmp),TokenAuth(token=TOKEN)))
            body,_ = self.request(base+'/health')
            self.assertEqual(json.loads(body),{'status':'UP'})
            self.request(base+'/')
            body,metadata = self.request(base+'/api/report',401)
            self.assertIn('Bearer',metadata['WWW-Authenticate'])
            self.assertNotIn(tmp.encode(),body)
            self.request(base+'/api/report?token='+TOKEN,401)
            self.request(base+'/api/report',401,OTHER)
            self.request(base+'/api/report',503,TOKEN)
            self.request(base+'/ready',503,TOKEN)
            self.request(base+'/api/report',403,TOKEN,{'Origin':'https://untrusted.example'})

    def test_exporter_auth_protects_metrics_and_readiness(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = self.server(exporter_handler(Path(tmp)/'control.db',Path(tmp)/'releases',TokenAuth(token=TOKEN)))
            self.request(base+'/health')
            for path in ['/metrics','/status','/ready']:
                self.request(base+path,401)
            data,_ = self.request(base+'/metrics',200,TOKEN)
            self.assertIn(b'bank_ingestion_available 0',data)
            self.request(base+'/status',503,TOKEN)
            self.request(base+'/ready',503,TOKEN)
            self.assertFalse((Path(tmp)/'control.db').exists())
