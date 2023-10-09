from setuptools import setup

setup(
    pbr=True,
    name='pyICU',
    version='0.0.1',
    description='Python package containing different functionalities for working with ICU data.',
    author='Christian Porschen',
    author_email='christian.porschen@ukmuenster.de',
    packages=['pyICU'],
    install_requires=[
        'colorlog>=6.7.0',
        'numpy>=1.25.2',
        'psycopg2>=2.9.7',
        'pandas>=2.0.3',
        'SQLAlchemy>=1.4.49',
    ],
)
