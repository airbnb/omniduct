# Omniduct

`omniduct` is a Python 2/3 package that provides a uniform interface for connecting to and extracting data from a wide variety of (potentially remote) data stores (including HDFS, Hive, Presto, MySQL, etc). It is especially useful in contexts where the data stores are only available via remote gateway nodes, where `omniduct` can automatically manage port forwarding over SSH to make these data stores available locally. It also provides convenient magic functions for use in IPython and Jupyter Notebooks.

`omniduct` has been extensively tested internally, but until our 1.0.0 release, we offer no guarantee of API stability.

Documentation for both users and developers will be arriving shortly, but the code is currently being offered for early adopters.

### Examples

**Create a presto client that connects via localhost**
```
In [1]: from omniduct import DuctRegistry

In [2]: duct_registry = DuctRegistry()

In [3]: presto_client = duct_registry.new('presto_local', protocol='presto', host='localhost', port=8080)

In [4]: presto_client.query("SELECT 42")
presto_local: Query: Complete after 0.14 sec on 2017-10-13.
Out[4]:
   _col0
0     42

In [5]: %%presto_local
    ...: {# magics are created and queries rendered using Jinja2 templating #}
    ...: SELECT 42
presto_local: Query: Complete after 1.20 sec on 2017-10-13.
Out[5]:
   _col0
0     42
```

**Create a presto client that connects via ssh to a remote server**

```
In [6]: duct_registry.new('my_server', protocol='ssh', host='<YOUR_SERVER_URL>', port=22)
Out[6]: <omniduct.remotes.ssh.SSHClient at 0x110bab550>

In [7]: duct_registry.new('presto_remote', protocol='presto', remote='my_server', port=8080)
Out[7]: <omniduct.databases.presto.PrestoClient at 0x110c04a58>

In [8]: %%presto_remote
    ...: SELECT 42
    ...:
presto_remote: Query: Connecting: Connected to localhost:8080 on <YOUR_SERVER_URL>.
INFO:pyhive.presto:SELECT 42
presto_remote: Query: Complete after 7.30 sec on 2017-10-13.
Out[8]:
   _col0
0     42
```
