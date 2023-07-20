from distutils.core import setup

setup(
    name='streamtools',
    version='0.1dev',
    packages=['streamtools',],
    install_requires=[
        'aiohttp==3.8.5',
        'aiokafka==0.5.1',
        'aio-pika==5.6.0',
        'boto3==1.19.8',
        'aiobotocore==2.0.1',
      ],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
)
