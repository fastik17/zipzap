from setuptools import setup, find_packages

setup(
    name='zipzap',
    version='0.1.0',
    description='zip-archive',
    author='fastik17',
    author_email='fastik17@example.com',
    url='https://github.com/fastik17/zipzap',
    install_requires=[
        'boto3',
        'requests',
        'environs',
    ],
    entry_points={
        'console_scripts': [
            'zip_upload = zip_upload:main',
        ],
    },
)
