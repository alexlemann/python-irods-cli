from setuptools import setup, find_packages

setup(
      name='python-irods-cli',
      version='0.1',
      packages=find_packages(),
      include_package_data=True,
      entry_points='''
          [console_scripts]
          pyirods=cli:main
      ''',
)
