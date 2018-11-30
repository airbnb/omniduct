__version__ = '1.0.0'
__author__ = 'John Smith'
__author_email__ = 'john.smith@company.com'

__dependencies__ = [
    # It is recommended to pin omniduct to a specific major version, and manually repin when updates go out
    # Omniduct is quite stable, but this provides a stronger guarantee of stability for users of your package
    'omniduct[ssh,presto,s3]>=1.0.0<1.1.0'
]
