from azure_data_lake import AzureDataLakeClient

TENANT_ID = "e66e77b4-5724-44d7-8721-06df160450ce"
USERNAME = "marcewo@umich.edu"
PASSWORD = "rssDgk2h%$r6p8D#"
STORE_NAME = "homework6"

client = AzureDataLakeClient(
    tenant_id=TENANT_ID,
    username=USERNAME,
    password=PASSWORD,
    store_name=STORE_NAME
)

client.connect()

# Test mkdir
client._mkdir("/test-directory", recursive=True, exist_ok=True)

# Test mkdir with nested directories
client._mkdir("/test-directory/nested-directory", recursive=True, exist_ok=True)

# Test write
client._write("/test-directory/test-file.txt", "Hello, World!")

# Test write in nested directory
client._write("/test-directory/nested-directory/test-file.txt", "Hello, Nested World!")

# Test read
content = client._read("/test-directory/test-file.txt")
print(content)

# Test read from nested directory
content_nested = client._read("/test-directory/nested-directory/test-file.txt")
print(content_nested)

# Test list
files = client._list("/test-directory")
print(files)

# Test list with details
files_details = client._list("/test-directory", details=True)
print(files_details)

# Test exists
print(client._exists("/test-directory/test-file.txt"))

# Test isdir
print(client._isdir("/test-directory"))

# Test isdir for nested directory
print(client._isdir("/test-directory/nested-directory"))

# Test isfile
print(client._isfile("/test-directory/test-file.txt"))

# Test isfile in nested directory
print(client._isfile("/test-directory/nested-directory/test-file.txt"))

# Test remove
client._remove("/test-directory/test-file.txt", recursive=True)

# Test remove file in nested directory
client._remove("/test-directory/nested-directory/test-file.txt", recursive=True)

# Test remove nested directory
client._remove("/test-directory/nested-directory", recursive=True)

# Test remove directory
client._remove("/test-directory", recursive=True)

client.disconnect()
