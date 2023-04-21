from azure_data_lake import AzureDataLakeClient

TENANT_ID = "e66e77b4-5724-44d7-8721-06df160450ce"
USERNAME = "marcewo@umich.edu"
PASSWORD = "rssDgk2h%$r6p8D#"
STORE_NAME = "homework6"

adl_client = AzureDataLakeClient(
    tenant_id=TENANT_ID,
    username=USERNAME,
    password=PASSWORD,
    store_name=STORE_NAME
)



adl_client.connect()
# Read the content of the file
with adl_client.open('/new-file.txt', mode='rb') as f:
    content = f.read().decode('utf-8')
print(content)
adl_client.disconnect()

