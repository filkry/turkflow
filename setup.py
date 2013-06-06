from distutils.core import setup

setup(
    name='turkflow',
    version='0.2.2',
    author='Fil Krynicki',
    author_email='filipkrynicki@gmail.com',
    packages=['turkflow'],
    package_data={'turkflow': ['templates/*']},
    license='LICENSE.txt',
    description='Library for creating Mechanical Turk workflows in python scripts.',
    long_description=open('README.md').read(),
    install_requires=[
                "networkx >= 1.7",
                "python-Levenshtein == 0.10.2",
                "boto >= 2.8.0",
                "Jinja2 >= 2.6",
                "numpy >= 1.7.1"
            ],
)
