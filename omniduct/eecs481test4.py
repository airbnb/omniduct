# Register AzureDataLakeClient with the DuctRegistry
from omniduct.filesystems.azure_data_lake import AzureDataLakeClient
from omniduct.registry import DuctRegistry

tenant_id = "e66e77b4-5724-44d7-8721-06df160450ce"
client_id = "9b280c81-500a-4b61-90c0-719b0762bbb5"
client_secret = "Z.t8Q~cBbOuYI~FrZOrDkd~tQzh3LVY_kHlbqcyQ"
store_name = "eecs481-GH"



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
print("Hello, World!")
test_dir = '/test_dir'
my_adl_client.mkdir(test_dir)
my_adl_client.disconnect()
print("Goodbye, World!")

