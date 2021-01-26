from setuptools import setup
from os import path

DIR = path.dirname(path.abspath(__file__))
# INSTALL_PACKAGES = open(path.join(DIR, 'requirements.txt')).read().splitlines()

with open(path.join(DIR, 'README.rst'),encoding='utf-8') as f:
    README = f.read()

setup(
    name='crown',
    packages=['crown'],
    description="crown is a simple and small ORM for Time Series Database (TSDB) tdengine(taos), making it easy to learn and intuitive to use.",
    long_description=README,
    long_description_content_type='text/markdown',
    install_requires=[
        'requests>=2.23.0'
    ],
    version='0.0.9',
    url='https://github.com/machine-w/crown',
    author='machine-w',
    author_email='steve2008.ma@gmail.com',
    keywords=['orm','taos', 'TDengine', 'TSDB','Time Series Database','connector','python'],
    tests_require=[
        'pytest',
        'pytest-faker',
        'pytest-sugar',
        'numpy',
        'pandas'
    ],
    package_data={
        # include json and txt files
        '': ['*.rst','*.txt'],
    },
    include_package_data=True,
    python_requires='>=3'
)