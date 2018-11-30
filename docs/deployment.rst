Deployment
==========

While Omniduct can be used on its own by manually constructing the services that
you need as part of your scripts and packages, it was designed specifically to
integrate well into a organisation-specific Python wrapper package that
preconfigures the services available within that organisation environment.
Typically such deployments would take advantage of Omniduct's `DuctRegistry` to
conveniently expose services within such a package.

An example wrapper package `is provided alongside the omniduct module`__ to help
bootstrap your own wrappers.

.. __: https://github.com/airbnb/omniduct/tree/master/example_wrapper

If you need any assistance, please do not hesitate to reach out to us via the
`GitHub issue tracker`__.

.. __: https://github.com/airbnb/omniduct/issues
