from distutils.core import setup

setup(
    name='streamtools',
    version='0.2',
    packages=['streamtools',],
    install_requires=[
        'aiohttp==3.7.4',
        'boto3==1.19.8',
        'aiobotocore==2.0.1',
      ],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
)
