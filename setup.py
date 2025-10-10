from distutils.core import setup

setup(
    name='streamtools',
    version='1.1.2',
    packages=['streamtools',],
    install_requires=[
        'aiohttp==3.13.0',
        'aiokafka==0.5.1',
        'aio-pika==5.6.0',
        'boto3==1.39.0',
        'aiobotocore==2.0.1',
      ],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
)
