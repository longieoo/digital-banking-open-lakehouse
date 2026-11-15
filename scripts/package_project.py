"""Package complete tracked source and working additions, without secrets or runtime artifacts."""
import argparse
import hashlib
import json
import re
from pathlib import Path
import subprocess
import tempfile
import zipfile

from scripts.run_release import code_revision


def selected(root):
    git = ['git','-c',f'safe.directory={root.as_posix()}','-C',str(root)]
    output = subprocess.check_output([*git,'ls-files','--cached','--others','--exclude-standard','-z'])
    paths = sorted(set(name.decode('utf-8') for name in output.split(b'\0') if name))
    forbidden = {'.git','.venv','runtime','work','__pycache__','.terraform','target','logs','build','dist'}
    files = []
    for name in paths:
        path = root/name
        if not path.is_file():
            raise ValueError('Tracked source is missing')
        if path.is_symlink() or path.resolve().is_relative_to(root.resolve()) is False:
            raise ValueError('Unsafe source path')
        env_secret = path.name.startswith('.env') and path.name not in {'.env.example'}
        if forbidden.intersection(Path(name).parts) or env_secret or path.suffix in {'.pem','.key','.plan'} or '.tfstate' in path.name:
            raise ValueError('Forbidden source artifact')
        if name=='PACKAGE_MANIFEST.json':
            raise ValueError('Reserved package manifest path')
        if path.suffix in {'.tfvars'} or path.name.endswith(('.bak','.backup','.old','~')):
            raise ValueError('Private configuration or stale backup file')
        files.append(name)
    return files


def package(root,output):
    root,output = Path(root).resolve(),Path(output).resolve()
    if output.exists():
        raise FileExistsError('Refusing to replace an existing delivery')
    names = selected(root)
    manifest = {'format':1,'code_revision':code_revision(root),'files':[]}
    output.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent) as temporary:
        archive = Path(temporary)/'source.zip'
        with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED,compresslevel=9) as bundle:
            for name in names:
                content = (root/name).read_bytes()
                if re.search(rb'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|(?:AKIA|ASIA)[A-Z0-9]{16}',content):
                    raise ValueError('Credential pattern in source')
                manifest['files'].append({'path':name,'bytes':len(content),'sha256':hashlib.sha256(content).hexdigest()})
                bundle.writestr('digital-banking-open-lakehouse/'+name,content)
            bundle.writestr('digital-banking-open-lakehouse/PACKAGE_MANIFEST.json',json.dumps(manifest,indent=2))
        with zipfile.ZipFile(archive) as bundle:
            if bundle.testzip() is not None:
                raise ValueError('Archive integrity failure')
            for item in manifest['files']:
                if hashlib.sha256(bundle.read('digital-banking-open-lakehouse/'+item['path'])).hexdigest()!=item['sha256']:
                    raise ValueError('Archive member mismatch')
        # Publish atomically without overwriting another process's delivery.
        import os
        os.link(archive,output)
    return {'status':'PASS','files':len(names),'bytes':output.stat().st_size,
            'sha256':hashlib.sha256(output.read_bytes()).hexdigest()}


if __name__=='__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True)
    args = parser.parse_args()
    print(json.dumps(package(Path(__file__).resolve().parent.parent,args.output)))
