from setuptools import setup


setup(name='codis',
      version='1.0',
      description='codis connection pool with py-redis',
      author="xshu",
      author_email="neilshu.sx@gmail.com",
      packages=['codis'],
      include_package_data=True,
      install_requires = [
        'redis',
        'kazoo'
      ]
)
