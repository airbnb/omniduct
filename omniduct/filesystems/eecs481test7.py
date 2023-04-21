from omniduct.filesystems.azure_data_lake import AzureDataLakeClient
from omniduct.registry import DuctRegistry


def test_adl_client():
    tenant_id = "e66e77b4-5724-44d7-8721-06df160450ce"
    username = "marcewo@umich.edu"
    password = "rssDgk2h%$r6p8D#"
    store_name = "homework6"

    # Initialize the registry
    registry = DuctRegistry()

    # Create an instance of AzureDataLakeClient
    adl_client = AzureDataLakeClient(tenant_id=tenant_id, username=username, password=password, store_name=store_name)

    # Register the client with the DuctRegistry
    registry.register(adl_client, name='my_adl_client')

    # Retrieve the client from the DuctRegistry
    my_adl_client = registry['my_adl_client']

    # Connect and test some operations
    my_adl_client.connect()

    # Test mkdir
    test_dir = '/test_dir'
    my_adl_client.mkdir(test_dir)
    assert my_adl_client.isdir(test_dir)

    # Test write and read
    content = 'Hello, world!'
    test_file = '/test_file'
    my_adl_client.write(test_file, content)
    assert my_adl_client.isfile(test_file)
    assert my_adl_client.read(test_file) == content

    # Test list
    files = ['file1.txt', 'file2.txt', 'file3.txt']
    for file in files:
        my_adl_client.write('/{}'.format(file), '')
    assert set(my_adl_client.list('/')) == set(files)

    # Test remove
    my_adl_client.remove(test_file)
    assert not my_adl_client.exists(test_file)

    # Test isdir and isfile
    my_adl_client.mkdir(test_dir)
    assert my_adl_client.isdir(test_dir)
    assert not my_adl_client.isfile(test_dir)

    # Test open
    with my_adl_client.open(test_file, mode='w') as f:
        f.write(content.encode())
    with my_adl_client.open(test_file, mode='r') as f:
        assert f.read() == content

    my_adl_client.disconnect()

if __name__ == '__main__':
    test_adl_client()
    
