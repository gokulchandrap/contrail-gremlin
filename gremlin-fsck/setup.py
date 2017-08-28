from setuptools import setup, find_packages

install_requires = [
    'gremlinpython==3.2.5',
]

test_requires = []

setup(
    name='gremlin-fsck',
    version='0.1',
    description="contrail-api-cli command to run live checks on gremlin server and fixes on api",
    long_description=open('README.md').read(),
    author="Jean-Philippe Braun",
    author_email="jean-philippe.braun@orange.com",
    maintainer="Jean-Philippe Braun",
    maintainer_email="jean-philippe.braun@orange.com",
    url="http://www.github.com/eonpatapon/gremlin-fsck",
    packages=find_packages(),
    install_requires=install_requires,
    scripts=[],
    license="MIT",
    entry_points={
        'contrail_api_cli.command': [
            'fsck = gremlin_fsck.fsck:Fsck',
        ]
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: User Interfaces',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7'
    ],
    keywords='contrail api cli',
    test_suite='contrail_fsck.tests'
)
