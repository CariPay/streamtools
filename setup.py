from distutils.core import setup

setup(
    name='streamtools',
    version='1.1.2',
    packages=['streamtools',],
    install_requires=[
        'aiohttp>=3.8.0,<4.0.0',
        'aiokafka>=0.8.0,<1.0.0',
        'aio-pika>=8.0.0,<10.0.0',
        'boto3>=1.26.0,<2.0.0',
        'aiobotocore>=2.5.0,<3.0.0',
      ],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
)
