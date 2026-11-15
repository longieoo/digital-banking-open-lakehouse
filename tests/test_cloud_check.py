import json
import unittest
from unittest.mock import Mock, patch

from bank.cloud_check import preflight, probe


class CloudCheckTest(unittest.TestCase):
    def setUp(self):
        self.config = {'STORAGE_BACKEND': 'azure', 'AZURE_STORAGE_ACCOUNT_NAME': 'syntheticbank',
                       'RAW_URI': 'abfss://raw@syntheticbank.dfs.core.windows.net/team',
                       'WAREHOUSE_URI': 'abfss://warehouse@syntheticbank.dfs.core.windows.net/',
                       'CATALOG_URI': 'https://catalog.example.org/v1',
                       'AZURE_CLIENT_SECRET': 'NEVER_PRINT_THIS_SECRET', 'AZURE_CLIENT_ID': 'PRIVATE_CLIENT',
                       'AZURE_TENANT_ID': 'PRIVATE_TENANT'}
        self.credential, self.service, self.blob = Mock(), Mock(), Mock()
        self.service.get_blob_client.return_value = self.blob
        self.blob.upload_blob.return_value = {'etag': 'owned-etag'}
        self.payload = None
        def upload(data, **kwargs):
            self.payload = data
            return {'etag': 'owned-etag'}
        self.blob.upload_blob.side_effect = upload
        self.blob.download_blob.return_value.readall.side_effect = lambda: self.payload
        self.factory = Mock(return_value=(self.credential, self.service, 'IfNotModified'))

    def run_probe(self):
        return probe(self.config, self.factory, lambda name: True)

    def test_offline_preflight_does_not_create_clients_or_echo_identity(self):
        with patch('bank.cloud_check._azure_clients') as factory:
            result = preflight(self.config, lambda name: True)
        factory.assert_not_called()
        self.assertEqual(result['status'], 'READY_FOR_PROBE')
        self.assertEqual(result['cloud_validation'], 'NOT_RUN')
        serialized = json.dumps(result)
        for private in ['NEVER_PRINT_THIS_SECRET', 'PRIVATE_CLIENT', 'PRIVATE_TENANT']:
            self.assertNotIn(private, serialized)

    def test_invalid_routing_missing_sdk_and_catalog_credentials_block_network(self):
        invalid = [
            {'RAW_URI': 'abfss://raw@otheraccount.dfs.core.windows.net/'},
            {'RAW_URI': 'abfss://raw@syntheticbank.dfs.core.windows.net/%2e%2e/existing'},
            {'WAREHOUSE_URI': self.config['RAW_URI']},
            {'CATALOG_URI': 'https://user:password@catalog.example.org/'},
            {'CATALOG_URI': 'http://catalog.example.org/'},
            {'CATALOG_URI': 'https://catalog.example.org/?token=secret'},
            {'AZURE_STORAGE_ACCOUNT_NAME': 'replaceuniquebankdemo'},
        ]
        for change in invalid:
            with self.subTest(change=change):
                result = probe({**self.config, **change}, self.factory, lambda name: True)
                self.assertEqual(result['status'], 'UNAVAILABLE')
        result = probe(self.config, self.factory, lambda name: False)
        self.assertEqual(result['status'], 'UNAVAILABLE')
        self.factory.assert_not_called()

    def test_probe_roundtrip_conditionally_deletes_only_its_own_etag(self):
        result = self.run_probe()
        self.assertEqual(result['status'], 'PASS')
        self.assertEqual(result['cloud_validation'], 'RAW_IO_ONLY')
        call = self.service.get_blob_client.call_args.kwargs
        self.assertEqual(call['container'], 'raw')
        self.assertEqual(call['blob'], f'team/_acceptance/{result["probe_id"]}.json')
        self.assertFalse(self.blob.upload_blob.call_args.kwargs['overwrite'])
        self.blob.delete_blob.assert_called_once_with(etag='owned-etag', match_condition='IfNotModified', timeout=30)
        self.service.close.assert_called_once()
        self.credential.close.assert_called_once()

    def test_read_failure_still_cleans_up_confirmed_own_blob_and_sanitizes_errors(self):
        self.blob.download_blob.side_effect = RuntimeError('NEVER_PRINT_THIS_SECRET')
        result = self.run_probe()
        self.assertEqual(result['status'], 'FAIL')
        self.assertTrue(result['deleted'])
        self.assertEqual(result['error'], {'stage': 'download', 'type': 'RuntimeError'})
        self.assertNotIn('NEVER_PRINT_THIS_SECRET', json.dumps(result))

    def test_failed_or_ambiguous_upload_does_not_delete_unconfirmed_blob(self):
        self.blob.upload_blob.side_effect = RuntimeError('ambiguous commit or preexisting blob')
        result = self.run_probe()
        self.assertEqual(result['status'], 'FAIL')
        self.assertEqual(result['cleanup'], 'NOT_ATTEMPTED_UPLOAD_OWNERSHIP_UNCONFIRMED')
        self.blob.delete_blob.assert_not_called()

    def test_changed_content_or_failed_etag_cleanup_does_not_claim_success(self):
        self.blob.download_blob.return_value.readall.side_effect = lambda: b'changed'
        result = self.run_probe()
        self.assertEqual(result['status'], 'FAIL')
        self.assertTrue(result['deleted'])
        self.blob.download_blob.return_value.readall.side_effect = lambda: self.payload
        self.blob.delete_blob.side_effect = RuntimeError('ETag changed or denied cleanup')
        result = self.run_probe()
        self.assertEqual(result['status'], 'FAIL')
        self.assertEqual(result['cleanup'], 'FAILED_OWN_PROBE_MAY_REMAIN')
        self.assertFalse(result['deleted'])
