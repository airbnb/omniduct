# Register AzureDataLakeClient with the DuctRegistry
from omniduct.filesystems.azure_data_lake import AzureDataLakeClient
from omniduct.registry import DuctRegistry
import unittest


tenant_id = "e66e77b4-5724-44d7-8721-06df160450ce"
client_id = "79389910-64e2-4b26-a81b-b5a4aa57b788"
client_secret = "Z.t8Q~cBbOuYI~FrZOrDkd~tQzh3LVY_kHlbqcyQ"
store_name = "eecs481-GH"





class TestAzureDataLakeClient(unittest.TestCase):

    def test_is_connected(self):
        client = AzureDataLakeClient(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret, store_name=store_name)
        self.assertFalse(client._is_connected())

        client.connect()
        self.assertTrue(client._is_connected())

        client.disconnect()
        self.assertFalse(client._is_connected())
        
        print(self._adls_account.url)

        
        
'''
class TestAzureDataLakeClient(unittest.TestCase):
    def test_listdir(self):
        client = AzureDataLakeClient(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret, store_name=store_name)
        client.connect()

        # Create a test directory
        test_dir = '/test_dir'
        client.mkdir(test_dir)

        # Create a test file inside the test directory
        test_file = test_dir + '/test_file.txt'
        client.write(test_file, 'Hello, World!')

        # Check that the test directory and file are listed
        self.assertIn('test_dir', client.listdir('/'))
        self.assertIn('test_file.txt', client.listdir(test_dir))

        # Remove the test directory and file
        client.remove(test_file)
        client.remove(test_dir)

        client.disconnect()


'''

        
if __name__ == '__main__':
    unittest.main()

