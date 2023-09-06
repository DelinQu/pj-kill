from setuptools import setup, find_packages
import os

requirements = [
    'colorama',
]

init_fn = os.path.join(os.path.dirname(__file__), 'pjkill', '__init__.py')
with open(init_fn) as f:
    for l in f.readlines():
        if '__version__' in l:
            exec(l)
            break

setup(
    name='pjkill',
    version=__version__,
    install_requires=requirements,
    python_requires='>=3.6',
    packages=find_packages(),
    author="DelinQu",
    author_email="delinqu.cs@gmail.com",
    entry_points={
        'console_scripts': [
            'pjkill = pjkill.pjkill:main',
        ]
    },
)