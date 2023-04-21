# Register AzureDataLakeClient with the DuctRegistry
from omniduct.filesystems.azure_data_lake import AzureDataLakeClient
from omniduct.registry import DuctRegistry

tenant_id = "e66e77b4-5724-44d7-8721-06df160450ce"
client_id = "79389910-64e2-4b26-a81b-b5a4aa57b788"
client_secret = "Z.t8Q~cBbOuYI~FrZOrDkd~tQzh3LVY_kHlbqcyQ"
store_name = "testtest"


# Initialize the registry
registry = DuctRegistry()

# Create an instance of AzureDataLakeClient
adl_client = AzureDataLakeClient(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret, store_name=store_name)

# Register the client with the DuctRegistry
registry.register(adl_client, name='my_adl_client')

# Retrieve the client from the DuctRegistry
my_adl_client = registry['my_adl_client']




# Connect and test some operations
my_adl_client.connect()
print(my_adl_client.listdir('/'))
my_adl_client.write('/test_file.txt', 'Hello, world!')
print(my_adl_client.read('/test_file.txt'))
my_adl_client.remove('/test_file.txt')
my_adl_client.disconnect()
