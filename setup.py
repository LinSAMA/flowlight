from setuptools import setup

with open('README.md', 'rb') as f:
    readme = f.read().decode('utf-8')

setup(
    name='flowlight',
    version='0.1.0',
    description='a tool make remote operations easier',
    long_description=readme,
    license='MIT',
    author='Wentao Liang',
    author_email='tonnie17@gmail.com',
    url='http://github.com/tonnie17/flowlight/',
    py_modules=['flowlight'],
    install_requires=['paramiko'],
    scripts=['bin/flowlight'],
    entry_points='''
    [console_scripts]
    flowlight=flowlight:api_serve
    '''
)
