from setuptools import setup
from setuptools import find_packages


req = ['polars==1.8.2',
     'requests==2.32.3']


setup(
    name = 'ecdata',
    version = '1.0.1',
    description='a pip installable package to distribute the Executive Communications Dataset',
    author = 'Joshua Allen',
    author_email='joshua.f.allen@gmail.com',
    license='CC-0',
    packages = ['ecdata'],
    url = 'https://github.com/joshuafayallen/executivecommunications-py',
    keywords=['Datasets'],
    python_requires = '>= 3.10',
    install_requires = req
)




