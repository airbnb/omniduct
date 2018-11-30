# Example Omniduct Wrapper

This is an example wrapper package around Omniduct to pre-configure services
availabe within a given organisation. Creating an organisation-specific Omniduct
configuration is a convenient way for people to make use of your organisation's
services via Python.

Services can be accessed using:
```
example_wrapper.services.databases.presto
```

Some services (in this example, `presto`) can been promoted to the top-level
module, and then can also be accessed using:
```
example_wrapper.presto
```

If you need any guidance when building your own wrapper, please open an issue in
the [GitHub issue tracker](https://github.com/airbnb/omniduct/issues).
