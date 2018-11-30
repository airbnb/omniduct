from setuptools import find_packages, setup

# Extract version information from Omniduct _version.py
version_info = {}
with open('example_wrapper/_version.py') as version_file:
    exec(version_file.read(), version_info)

setup(
    # Package metadata
    name="example_wrapper",
    version=version_info['__version__'],
    author=version_info['__author__'],
    author_email=version_info['__author_email__'],
    url='http://git.company.com/example_wrapper',
    description="Exposes Company.com services locally.",
    license='Internal',

    # Package details
    packages=find_packages(),
    include_package_data=True,

    # Dependencies
    install_requires=version_info['__dependencies__']
)
