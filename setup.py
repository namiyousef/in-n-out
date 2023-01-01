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

def parse_requirements(path_to_file):
    with open(path_to_file) as f:
        requirements = f.readlines()
    
    return requirements
    
test_packages = [
    'pytest',
    'coverage',
    'pytest-dependency'
]

core_packages = parse_requirements('requirements/core.txt')

setup(
    name='in_n_out',
    version=get_version("in_n_out/__init__.py"),
    description='A package for exploring data instance effects in transformer models',
    author='Yousef Nami',
    author_email='namiyousef@hotmail.com',
    url='https://github.com/namiyousef/in-n-out',
    install_requires=core_packages,
    test_require=test_packages,
    packages=find_packages(exclude=('tests*', 'experiments*')),
    #package_data={'': ['api/specs/api.yaml']},
    include_package_data=True,
    license='MIT',
    #entry_points={
    #    'console_scripts': ['in-n-out-api=in_n_out.run_api:'],
    #}
)