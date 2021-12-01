from setuptools import setup, find_packages

setup(
    name='estimate_start_times',
    version='0.2.0',
    packages=find_packages(where='estimate_start_times'),
    package_dir={"": "estimate_start_times"},
    include_package_data=True,
    install_requires=[
        'pandas',
        'numpy',
        'pm4py',
        'statistics',
    ],
    entry_points={
        'console_scripts': [
            'estimate_start_times = main:main',
        ]
    }
)
