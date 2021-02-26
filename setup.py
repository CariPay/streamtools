from distutils.core import setup

setup(
    name='streamtools',
    version='0.1dev',
    packages=['streamtools',],
    install_requires=[
        'aiohttp==3.7.4',
        'aiokafka==0.5.1',
        'aio-pika==5.6.0',
        'boto3==1.12.32',
        'aiobotocore==1.0.4',
      ],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
)
