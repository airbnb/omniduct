# Import the required modules
from omniduct.filesystems.azure_data_lake import AzureDataLakeClient
from omniduct.registry import DuctRegistry

# Set the required credentials and store name
tenant_id = "e66e77b4-5724-44d7-8721-06df160450ce"
client_id = "79389910-64e2-4b26-a81b-b5a4aa57b788"
client_secret = "382888b2-adf8-43cf-8818-8b3acd086361"
store_name = "eecs481-GH"

# Create a new instance of the DuctRegistry
registry = DuctRegistry()

# Create a new instance of the AzureDataLakeClient and register it with the DuctRegistry
adl_client = AzureDataLakeClient(tenant_id, client_id, client_secret, store_name)
registry.register(adl_client, name='my_adl_client')

# Retrieve the client from the DuctRegistry
my_adl_client = registry['my_adl_client']

# Connect to the Azure Data Lake Storage Gen2 and perform some operations
my_adl_client.connect()
print(my_adl_client.listdir('/'))
my_adl_client.write('/test_file.txt', 'Hello, world!')
print(my_adl_client.read('/test_file.txt'))
my_adl_client.remove('/test_file.txt')
my_adl_client.disconnect()
