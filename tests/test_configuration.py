import importlib.util
from pathlib import Path
import unittest


@unittest.skipUnless(importlib.util.find_spec('yaml'),'Install config test dependencies')
class ConfigurationTest(unittest.TestCase):
    def test_mounts_builds_profiles_and_persistence(self):
        import yaml
        config=yaml.safe_load(Path('docker-compose.yml').read_text())
        services=config['services']
        for name,service in services.items():
            self.assertTrue(service.get('profiles'), name)
            if 'build' in service:
                self.assertTrue(Path(service['build']['dockerfile']).is_file())
            for mount in service.get('volumes',[]):
                source=mount.split(':')[0]
                if source.startswith('./') and source != './runtime' and not source.startswith('./runtime/'):
                    self.assertTrue(Path(source).exists(),source)
            for dependency in service.get('depends_on',{}):
                self.assertIn(dependency,services)
        for name in ['postgres','minio','catalog','redpanda','airflow-scheduler','trino']:
            self.assertIn('healthcheck',services[name])

    def test_readme_links_resolve(self):
        import re
        for name in ['README.md','docs/demo.md','docs/coursework/README.md']:
            path=Path(name)
            for link in re.findall(r'\]\(([^)]+)\)',path.read_text(encoding='utf-8')):
                if '://' not in link and not link.startswith('#'):
                    self.assertTrue((path.parent/link.split('#')[0]).exists(), f'{name}: {link}')
