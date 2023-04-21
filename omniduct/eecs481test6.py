from azure.datalake.store import core, lib

tenant_id = "e66e77b4-5724-44d7-8721-06df160450ce"
username = "marcewo@umich.edu"
password = "rssDgk2h%$r6p8D#"
store_name = "homework6"

token = lib.auth(tenant_id, username, password)
adl = core.AzureDLFileSystem(token, store_name=store_name)

file_path = '/new-file2.txt'
with adl.open(file_path, 'wb') as f:
    f.write(b'Hello, world!')



'''
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
'''
