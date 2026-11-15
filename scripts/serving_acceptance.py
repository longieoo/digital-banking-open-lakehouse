"""Real HTTP acceptance for certified BI and operational exporter; no browser substitute claim."""
import argparse
from decimal import Decimal
import json
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def fetch(url,status=200,json_body=True,token=None):
    try:
        request = Request(url,headers={'Authorization':'Bearer '+token} if token else {})
        with urlopen(request,timeout=30) as response:
            actual,content = response.status,response.read().decode('utf-8')
    except HTTPError as exc:
        actual,content = exc.code,exc.read().decode('utf-8')
    if actual!=status:
        raise ValueError('Unexpected HTTP acceptance status')
    return json.loads(content) if json_body else content


def check(bi,monitoring,token=None):
    if fetch(bi+'/health')!={'status':'UP'}:
        raise ValueError('Unexpected liveness data')
    health = fetch(bi+'/ready',token=token)
    report = fetch(bi+'/api/report',token=token)
    if health['release_id']!=report['provenance']['release_id']:
        raise ValueError('Release changed during acceptance; retry')
    result = report['summary']
    if result['reconciliation_groups']<1 or result['reconciliation_failed']!=0:
        raise ValueError('Reporting reconciliation failed')
    for name in ('transaction_value','posted_value','net_posted_movement','closing_balance'):
        if not isinstance(result[name],str) or not Decimal(result[name]).is_finite():
            raise ValueError('Invalid exact monetary API result')
    fetch(bi+'/api/report?currency=INVALID',400,token=token)
    fetch(bi+'/api/report?start=not-a-date',400,token=token)
    fetch(bi+'/api/report?currency=USD&currency=EUR',400,token=token)
    fetch(bi+'/unpublished-file',404,token=token)
    ready = fetch(monitoring+'/ready',token=token)
    if not ready['ready'] or ready['release']['current_release_id']!=health['release_id']:
        raise ValueError('Readiness does not match reporting release')
    data = fetch(monitoring+'/metrics',json_body=False,token=token)
    if token:
        fetch(bi+'/api/report',401)
        fetch(monitoring+'/metrics',401)
    numeric = [line for line in data.splitlines() if line and not line.startswith('#')]
    for line in numeric:
        _,value = line.split()
        if not Decimal(value).is_finite():
            raise ValueError('Non-finite operational metric')
    return {'status':'PASS','release_id':health['release_id'],'summary':result,
            'invalid_filter_rejection':'PASS','numeric_metrics':len(numeric),
            'authentication':'PASS' if token else 'DISABLED_LOCAL_MODE',
            'visual_browser_qa':'NOT_VERIFIED'}


if __name__=='__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bi',default='http://127.0.0.1:8090')
    parser.add_argument('--metrics',default='http://127.0.0.1:9091')
    parser.add_argument('--token-file',help='Private host token file; the value is never logged')
    args = parser.parse_args()
    token = Path(args.token_file).read_text(encoding='ascii').strip() if args.token_file else None
    print(json.dumps(check(args.bi,args.metrics,token),indent=2))
