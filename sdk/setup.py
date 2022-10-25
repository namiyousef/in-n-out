from setuptools import setup, find_packages
import codecs
import os.path

def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()

def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


setup(
    name='in-n-out-sdk',
    version=get_version("in_n_out_sdk/__init__.py"),
    description='sdk for in_n_out',
    author='Yousef Nami',
    author_email='namiyousef@hotmail.com',
    url='https://github.com/namiyousef/in-n-out',
    install_requires=[
        'pandas',
        'requests',
        'pyarrow'
    ],
    packages=find_packages(exclude=('tests*', 'experiments*')),
    #package_data={'': ['api/specs/api.yaml']},
    include_package_data=True,
    license='MIT',
    #entry_points={
    #    'console_scripts': ['in-n-out-api=in_n_out.run_api:'],
    #}
)