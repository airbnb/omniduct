import unittest
import mock
from pyfakefs.fake_filesystem_unittest import Patcher

from omniduct.caches.local import LocalCache


ID_DUCT = 'test_id_duct'
ID_STRING = 'test_id_string'
ID_STRING_ANOTHER = 'test_id_string_another'
ID_STRING_NONEXISTANT = 'test_id_string_nonexistant'
TEST_DIR = 'test_dir'


class TestLocalCache(unittest.TestCase):

    def setUp(self):
        self.fs_patcher = Patcher()
        self.fs_patcher.setUp()
        self.cache = LocalCache(TEST_DIR)

    def test_clear(self):
        self.cache.clear(ID_DUCT, ID_STRING_NONEXISTANT)
        self.cache.set(ID_DUCT, ID_STRING, 'foo')
        self.cache.clear(ID_DUCT, ID_STRING)
        self.assertFalse(
            self.cache.has_key(ID_DUCT, ID_STRING),
            'expected not to find key if key is cleared'
        )
        self.assertIsNone(
            self.cache.get(ID_DUCT, ID_STRING),
            'expected to get None if key has been cleared'
        )

    def test_clear_all(self):
        self.cache.set(ID_DUCT, ID_STRING, 'foo')
        self.cache.set(ID_DUCT, ID_STRING_ANOTHER, 'bar')
        self.cache.clear_all(ID_DUCT)

        self.assertFalse(
            self.cache.has_key(ID_DUCT, ID_STRING),
            'expected not to find key after clear_all'
        )
        self.assertFalse(
            self.cache.has_key(ID_DUCT, ID_STRING_ANOTHER),
            'expected not to find key after clear_all'
        )

    def test_get(self):
        self.assertIsNone(
            self.cache.get(ID_DUCT, ID_STRING_NONEXISTANT),
            'expected not find non-existant key'
        )
        self.cache.set(ID_DUCT, ID_STRING, 'foo')
        self.assertEqual(
            self.cache.get(ID_DUCT, ID_STRING), 'foo',
            'expected object retrieved from cache to be equal to its pre-cached value'
        )

    def test_has_key(self):
        self.assertFalse(
            self.cache.has_key(ID_DUCT, ID_STRING_NONEXISTANT),
            'expected not to find non-existant key'
        )
        self.cache.set(ID_DUCT, ID_STRING, 'foo')
        self.assertTrue(
            self.cache.has_key(ID_DUCT, ID_STRING),
            'expected to find key after setting it'
        )

    def test_keys(self):
        pass  # TODO test keys when implemented

    def test_set(self):
        serializer_mock = mock.Mock()
        self.cache.set(ID_DUCT, ID_STRING, 'foo', serializer=serializer_mock.serialize)
        serializer_mock.serialize.assert_called_once()

    def tearDown(self):
        self.fs_patcher.tearDown()
