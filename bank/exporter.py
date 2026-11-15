"""Read-only local HTTP health and Prometheus exporter; never creates control state."""
import argparse
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sqlite3

from bank.observability import metrics, release_metrics
from bank.http_security import TokenAuth, security_headers


class ReadOnlyState:
    def __init__(self,path):
        self.path = Path(path)

    def manifests(self):
        with closing(sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute('SELECT * FROM batches ORDER BY started_at')]


def collect(control, releases):
    result = {'ingestion':{'available':0},'release':{'available':0}}
    for key,operation in [('ingestion',lambda:metrics(ReadOnlyState(control))),
                          ('release',lambda:release_metrics(releases))]:
        try:
            values = operation()
            result[key] = {'available':1,**values}
        except (OSError,ValueError,KeyError,TypeError,sqlite3.Error):
            # No exception messages, paths, credentials or raw rows are exposed.
            pass
    result['ready'] = bool(result['ingestion']['available'] and
                           result['release'].get('certified_current')==1)
    return result


def prometheus(result):
    lines = []
    for scope in ('ingestion','release'):
        for name,value in result[scope].items():
            if isinstance(value,(int,float)):
                metric = f'bank_{scope}_{name}'
                lines.extend([f'# TYPE {metric} gauge',f'{metric} {value}'])
    return '\n'.join(lines)+'\n'


def handler(control,releases,auth=None):
    auth = auth or TokenAuth.from_env()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):
            pass

        def do_GET(self):
            accepted = auth.request_status(self.headers,protected=self.path!='/health')
            if accepted!=200:
                status,content,kind = accepted,b'{"error":"Request rejected"}','application/json'
            elif self.path=='/health':
                status,content,kind = 200,b'{"status":"UP"}', 'application/json'
            elif self.path in {'/metrics','/ready','/status'}:
                result = collect(control,releases)
                if self.path=='/metrics':
                    status,content,kind = 200,prometheus(result).encode(),'text/plain; version=0.0.4'
                else:
                    status = 200 if result['ready'] else 503
                    content,kind = json.dumps(result).encode(),'application/json'
            else:
                status,content,kind = 404,b'{"error":"NotFound"}','application/json'
            self.send_response(status)
            self.send_header('Content-Type',kind)
            self.send_header('Content-Length',str(len(content)))
            security_headers(self,status)
            self.end_headers()
            self.wfile.write(content)
    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=9091)
    parser.add_argument('--control',default=os.environ.get('CONTROL_PATH','runtime/control.db'))
    parser.add_argument('--root',default=os.environ.get('RELEASE_ROOT','runtime/releases'))
    parser.add_argument('--require-auth',action='store_true',help='Fail startup without a valid HTTP token')
    args = parser.parse_args()
    try:
        auth = TokenAuth.from_env(require=args.require_auth)
    except ValueError:
        parser.error('HTTP authentication configuration is missing, invalid or expired')
    with ThreadingHTTPServer((args.host,args.port),handler(args.control,args.root,auth)) as server:
        server.serve_forever()


if __name__=='__main__':
    main()
