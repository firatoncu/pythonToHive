"""Setup for the chocobo package."""

import setuptools



setuptools.setup(
    author="Firat Oncu",
    author_email="f.firatoncu@gmail.com",
    name='pythonToHive',
    license="MIT",
    description='for a better world.',
    version='v0.0.1',
    long_description="no need",
    url='https://github.com/firatoncu/pythonToHive',
    packages=setuptools.find_packages(),
    python_requires=">=3.5",
    install_requires=['requests'],
    classifiers=[
        # Trove classifiers
        # (https://pypi.python.org/pypi?%3Aaction=list_classifiers)
        'Development Status :: 1 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Intended Audience :: Developers',
    ],
)
