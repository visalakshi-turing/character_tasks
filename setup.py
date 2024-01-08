from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name='src',  # Replace with your project/package name
    version='0.1.0',  # Version of your package
    package_dir={'': 'src'},  # Specifies that packages are under src
    packages=find_packages(where='src'),  # Finds all packages in src
    install_requires=requirements,  # Optional: Specify package dependencies
)
