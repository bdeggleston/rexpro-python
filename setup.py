from setuptools import setup, find_packages

#next time:
#python setup.py register
#python setup.py sdist upload

version = "0.1"

long_desc = """
Experimental Python RexPro interface
"""

setup(
    name='rexpro',
    version=version,
    description='Python RexPro interface',
    dependency_links=['https://github.com/bdeggleston/rexpro-python/archive/{0}.tar.gz#egg=rexpro-python-{0}'.format(version)],
    long_description=long_desc,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        'Topic :: Database',
        'Topic :: Database :: Front-Ends',
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords='rexster,tinkerpop,rexpro',
    install_requires=['msgpack-python'],
    author='Blake Eggleston',
    author_email='bdeggleston@gmail.com',
    url='https://github.com/bdeggleston/rexpro-python',
    license='BSD',
    packages=find_packages(),
    include_package_data=True,
)

