import unittest
from unittest.mock import patch

from common.recognition_database_readiness import check_connection


class RecognitionDatabaseReadinessTests(unittest.TestCase):
    def _connection(self, *, vendor='postgresql', host='127.0.0.1', port=55476,
                    peer='127.0.0.1', server_port=55476, server_version='16.4',
                    wire_peer='127.0.0.1'):
        class Features:
            pass
        class Connection:
            settings_dict = {'HOST': host, 'PORT': str(port)}
            features = Features()
            def __init__(self):
                self.vendor = vendor
                self.info = type('Info', (), {
                    'host': host, 'port': port, 'hostaddr': wire_peer,
                })()
            def cursor(self):
                return self
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def execute(self, query, params=None):
                self.query = query
            def fetchone(self):
                return (peer, server_port, server_version)
        return Connection()

    def test_direct_peer_to_verified_postgres_container_passes(self):
        connection = self._connection()
        self.assertTrue(check_connection(connection, {'127.0.0.1'}, 55476))

    def test_different_peer_is_rejected(self):
        connection = self._connection(peer='10.0.0.9')
        self.assertFalse(check_connection(connection, {'127.0.0.1'}, 55476))

    def test_proxy_peer_is_rejected_even_when_configured_host_matches(self):
        connection = self._connection(peer='127.0.0.1', server_port=6432)
        self.assertFalse(check_connection(connection, {'127.0.0.1'}, 55476))

    def test_proxy_wire_peer_is_rejected_even_when_backend_address_matches(self):
        connection = self._connection(wire_peer='10.0.0.9')
        self.assertFalse(check_connection(connection, {'127.0.0.1'}, 55476))

    def test_missing_or_unsupported_metadata_is_rejected(self):
        self.assertFalse(check_connection(self._connection(vendor='sqlite'), {'127.0.0.1'}, 55476))
        connection = self._connection()
        with patch.object(connection, 'cursor', side_effect=AttributeError('missing')):
            self.assertFalse(check_connection(connection, {'127.0.0.1'}, 55476))


if __name__ == '__main__':
    unittest.main()
