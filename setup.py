import os
from setuptools import setup

README = """
See the README on `GitHub
<https://github.com/uw-it-aca/edw-clients>`_.
"""

version_path = 'edw_clients/VERSION'
VERSION = open(os.path.join(os.path.dirname(__file__), version_path)).read()
VERSION = VERSION.replace("\n", "")

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

url = "https://github.com/uw-it-aca/edw-clients"
setup(
    name='edw-clients',
    version=VERSION,
    packages=['edw_clients'],
    author="UW-IT AXDD",
    author_email="aca-it@uw.edu",
    include_package_data=True,
    install_requires=[
        'commonconf~=1.0',
        'mock',
        'pymssql==2.2.2',
        'pandas~=1.1',
        'SQLAlchemy==1.0.14'
    ],
    license='Apache License, Version 2.0',
    description=('A library of EDW clients'),
    long_description=README,
    url=url,
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6'
    ],
)
