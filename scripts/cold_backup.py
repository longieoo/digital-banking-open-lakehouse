"""Offline Docker recovery for one local Compose project. Use a private backup directory."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile

VOLUMES = ('postgres-data', 'minio-data', 'catalog-data', 'redpanda-data', 'airflow-data')
IMAGE = 'postgres:16.9'


def docker(*args):
    return subprocess.check_output(['docker', *args], text=True).strip()


def volumes(project):
    return set(docker('volume', 'ls', '--filter', f'label=com.docker.compose.project={project}',
                      '--format', '{{.Name}}').splitlines())


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def transfer(source, archive, restoring):
    kind = 'bind' if isinstance(source, Path) else 'volume'
    mount = f'type={kind},src={source},dst=/data' + ('' if restoring else ',readonly')
    args = ['docker', 'run', '--rm', '-i', '--network', 'none', '--mount', mount, '--entrypoint', 'tar', IMAGE]
    with archive.open('rb' if restoring else 'xb') as handle:
        if restoring:
            subprocess.run([*args, '-xpf', '-', '-C', '/data'], stdin=handle, check=True)
        else:
            subprocess.run([*args, '-cpf', '-', '-C', '/data', '.'], stdout=handle, check=True)


def validate_tar(path):
    with tarfile.open(path, 'r:') as data:
        for entry in data:
            name = PurePosixPath(entry.name)
            if name.is_absolute() or '..' in name.parts or not (entry.isfile() or entry.isdir()):
                raise ValueError('Unsupported archive member')


def run(action, project, runtime, bundle):
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]*', project):
        raise ValueError('Invalid Compose project')
    runtime, bundle = runtime.resolve(), bundle.resolve()
    if runtime == bundle or runtime in bundle.parents or bundle in runtime.parents:
        raise ValueError('Runtime and backup directories must be separate')
    if docker('ps', '-q', '--filter', f'label=com.docker.compose.project={project}'):
        raise ValueError('Stop the selected Compose project before cold recovery')
    existing = volumes(project)
    if action == 'backup':
        if not runtime.is_dir():
            raise ValueError('Missing runtime')
        containers = docker('ps','-aq','--filter',f'label=com.docker.compose.project={project}').splitlines()
        if not containers:
            raise ValueError('Cannot associate runtime with the stopped project')
        # Desktop mounts may report /run/desktop/mnt/host/c/... instead of C:/....
        expected = runtime.as_posix()
        aliases = {expected}
        if runtime.drive:
            drive_path = runtime.drive[0].lower() + expected[2:]
            aliases.update(prefix + drive_path for prefix in ('/run/desktop/mnt/host/', '/host_mnt/'))
        mounts = [mount for container in containers for mount in
                  json.loads(docker('inspect','--format','{{json .Mounts}}',container))]
        if not any(mount['Destination'] in {'/runtime','/opt/bank/runtime'} and
                   mount['Source'].replace('\\\\','/') in aliases for mount in mounts):
            raise ValueError('Runtime path does not match the selected project mounts')
        selected = [v for v in VOLUMES if f'{project}_{v}' in existing]
        if not {'postgres-data','minio-data','catalog-data'} <= set(selected):
            raise ValueError('Missing core volumes')
        bundle.mkdir(parents=True, exist_ok=False)
        hashes = {}
        for key in ['runtime', *selected]:
            path = bundle / f'{key}.tar'
            transfer(runtime if key == 'runtime' else f'{project}_{key}', path, False)
            validate_tar(path)
            hashes[key] = digest(path)
        (bundle / 'manifest.json').write_text(json.dumps({'format': 1, 'hashes': hashes}, indent=2))
    else:
        manifest = json.loads((bundle / 'manifest.json').read_text())
        hashes = manifest['hashes']
        if manifest['format'] != 1 or not {'runtime','postgres-data','minio-data','catalog-data'} <= hashes.keys() or hashes.keys() - {'runtime', *VOLUMES}:
            raise ValueError('Invalid backup inventory')
        names = set(docker('volume','ls','--format','{{.Name}}').splitlines())
        if existing or runtime.exists() or any(f'{project}_{v}' in names for v in VOLUMES):
            raise ValueError('Restore requires an unused project and new runtime path')
        for key, expected in hashes.items():
            path = bundle / f'{key}.tar'
            if digest(path) != expected:
                raise ValueError('Backup checksum mismatch')
            validate_tar(path)
        runtime.mkdir(parents=True, exist_ok=False)
        for key in hashes:
            target = runtime if key == 'runtime' else docker('volume','create',
                '--label',f'com.docker.compose.project={project}', '--label',f'com.docker.compose.volume={key}',
                f'{project}_{key}')
            transfer(target, bundle / f'{key}.tar', True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['backup','restore'])
    parser.add_argument('--project', required=True)
    parser.add_argument('--runtime', type=Path, required=True)
    parser.add_argument('--bundle', type=Path, required=True)
    args = parser.parse_args()
    run(args.action, args.project, args.runtime, args.bundle)
