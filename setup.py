from setuptools import find_packages, setup

setup(
    name='teleinflux',
    version='0.1.1',
    description='Teleinfo to InfluxDB',
    url='https://github.com/Kaldor37/TeleInflux',
    author='Kaldor37',
    author_email='davy.gabard@gmail.com',
    packages=find_packages(),
    install_requires=[
        'influxdb>=5.3',
        'pyserial>=3.5'
    ]
)
