from setuptools import setup, find_namespace_packages

with open('README.md') as f:
    readme = f.read()

setup(
    name='py-modules',
    version='0.1.0',
    description='Python helper modules for working with various tools.',
    long_description=readme,
    author='Emruz Hossain',
    author_email='hossainemruz@gmail.com',
    url='https://github.com/bd-ml/py-modules',
    packages=find_namespace_packages(include=('rabbitmq', 'minio_client')),
    install_requires=[
        "pika",
        "git+https://github.com/minio/minio-py@7.1.2#egg=minio",
    ]
)
