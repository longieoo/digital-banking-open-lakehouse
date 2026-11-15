from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import platform


class PlatformTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        previous = Path.cwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, previous)
        environment = patch.dict(os.environ, {}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)
        Path('.env').write_text('KEEP_EXISTING_ENV=unchanged\n')

    def run_main(self, *args, side_effect=None):
        output = io.StringIO()
        with patch.object(sys, 'argv', ['platform', *args]), redirect_stdout(output), \
                patch('scripts.platform.subprocess.run', side_effect=side_effect,
                      return_value=SimpleNamespace(returncode=0)) as run:
            status = platform.main()
        return status, [tuple(call.args[0]) for call in run.call_args_list], output.getvalue()

    def test_up_preserves_env_and_does_not_seed_without_explicit_flag(self):
        before = Path('.env').read_bytes()
        result, commands, output = self.run_main('up', '--runtime', 'isolated', '--project', 'safe-demo')
        self.assertEqual(result, 0)
        self.assertEqual(Path('.env').read_bytes(), before)
        self.assertFalse(any('seed' in command for command in commands))
        self.assertFalse(any('scripts/create_demo_env.py' in command for command in commands))
        self.assertEqual(commands[0], ('docker', 'compose', 'config', '--quiet'))
        release = next(i for i, command in enumerate(commands) if 'scripts.run_release' in command)
        query = next(i for i, command in enumerate(commands) if 'trino' in command)
        dashboard = next(i for i, command in enumerate(commands) if 'dashboard' in command)
        self.assertLess(query, release)
        self.assertLess(release, dashboard)
        self.assertEqual(json.loads(output)['status'], 'STARTED')
        self.assertEqual(os.environ['COMPOSE_PROJECT_NAME'], 'safe-demo')
        self.assertEqual(Path(os.environ['BANK_RUNTIME_DIR']), self.root / 'isolated')

    def test_all_starts_workers_after_dependencies_and_only_explicit_seed(self):
        def execute(command, **kwargs):
            return SimpleNamespace(returncode=1 if 'describe' in command else 0)
        result, commands, _ = self.run_main('up', '--all', '--internal', '--seed',
                                           '--runtime', 'isolated', '--project', 'complete-demo',
                                           side_effect=execute)
        self.assertEqual(result, 0)
        self.assertIn('airflow-init', next(command for command in commands if 'build' in command))
        self.assertIn(('docker', 'compose', 'run', '--rm', 'bank', 'seed'), commands)
        broker = next(i for i, command in enumerate(commands) if 'redpanda' in command and 'up' in command)
        self.assertIn('airflow-scheduler', commands[broker])
        self.assertIn('airflow-webserver', commands[broker])
        topic = next(i for i, command in enumerate(commands) if 'create' in command and 'bank.transactions' in command)
        dimensions = next(i for i, command in enumerate(commands) if 'processing/silver.py' in command)
        producer = next(i for i, command in enumerate(commands) if 'streaming.producer' in command)
        consumer = next(i for i, command in enumerate(commands) if 'stream-consumer' in command)
        self.assertLess(broker, topic)
        self.assertLess(topic, dimensions)
        self.assertLess(dimensions, producer)
        self.assertLess(producer, consumer)
        self.assertEqual(os.environ['COMPOSE_FILE'], os.pathsep.join([
            'docker-compose.yml', 'docker-compose.ci.yml']))
        self.assertFalse(any('down' in command or '--volumes' in command for command in commands))

    def test_stop_and_status_never_build_seed_or_delete_volumes(self):
        for action, suffix in [('stop', ('stop',)), ('status', ('ps', '--all'))]:
            result, commands, _ = self.run_main(action, '--project', 'selected-project')
            self.assertEqual(result, 0)
            self.assertEqual(commands, [('docker', 'compose', '--profile', '*', *suffix)])
            self.assertEqual(os.environ['COMPOSE_PROJECT_NAME'], 'selected-project')

    def test_no_release_starts_serving_without_building_reporting_data(self):
        Path('.env').unlink()
        _, commands, _ = self.run_main('up', '--no-release')
        self.assertEqual(commands[0][1:], ('scripts/create_demo_env.py',))
        self.assertFalse(any('scripts.run_release' in command or 'seed' in command for command in commands))
        self.assertTrue(any('dashboard' in command for command in commands))

    def test_missing_docker_is_reported_without_exception_or_secret_text(self):
        for path in ['docker-compose.yml', 'dbt/dbt_project.yml', 'sources/schema.sql']:
            file = Path(path)
            file.parent.mkdir(parents=True, exist_ok=True)
            file.touch()
        with patch('scripts.platform.importlib.util.find_spec', return_value=object()), \
                patch('scripts.platform.subprocess.run', side_effect=FileNotFoundError('PRIVATE_RUNTIME_DETAIL')):
            result = platform.doctor()
        self.assertEqual(result['status'], 'INCOMPLETE')
        self.assertFalse(result['checks']['docker_engine'])
        self.assertNotIn('PRIVATE_RUNTIME_DETAIL', json.dumps(result))

    def test_start_failure_stops_before_success_or_later_services(self):
        commands, output = [], io.StringIO()
        def execute(command, **kwargs):
            commands.append(tuple(command))
            if 'build' in command:
                raise subprocess.CalledProcessError(7, command)
            return SimpleNamespace(returncode=0)
        with patch.object(sys, 'argv', ['platform', 'up']), redirect_stdout(output), \
                patch('scripts.platform.subprocess.run', side_effect=execute):
            with self.assertRaises(subprocess.CalledProcessError):
                platform.main()
        self.assertFalse(any('up' in command for command in commands))
        self.assertNotIn('STARTED', output.getvalue())
