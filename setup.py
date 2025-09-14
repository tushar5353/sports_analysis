from setuptools import setup, find_packages

setup(
    name='sports_analysis',               # Your package name
    version='1.0',                   # Initial version
    description='A sample Python package to run the production grade service',
    author='Tushar Sharma',
    author_email='tushar5353@gmail.com',
    packages=find_packages(),        # Finds all packages automatically
    install_requires=[
        'numpy',
        'pandas',
        'psycopg2-binary',
        'apache-airflow-providers-redis',
        'redis<4.0.0'
        # Add your dependencies here
    ],
)

