"""Create one private local serving token under ignored runtime; never print it."""
import argparse
import os
from pathlib import Path
import secrets


def create_token(output):
    path = Path(output)
    runtime = Path('runtime').resolve()
    if not path.resolve().is_relative_to(runtime) or any(p.is_symlink() for p in (path,*path.parents)):
        raise ValueError('HTTP secrets must stay inside the local runtime directory')
    path.parent.mkdir(parents=True,exist_ok=True)
    descriptor = os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(descriptor,'w',encoding='ascii') as handle:
        handle.write(secrets.token_urlsafe(48)+'\n')
        handle.flush()
        os.fsync(handle.fileno())
    return {'status':'CREATED','token_logged':False}


if __name__=='__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',default='runtime/secrets/http-token')
    args = parser.parse_args()
    try:
        import json
        print(json.dumps(create_token(args.output)))
    except (OSError,ValueError):
        raise SystemExit('Token file creation failed; existing secret preserved') from None
