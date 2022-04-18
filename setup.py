from setuptools import setup

setup(
    name='estimate_start_times',
    version='0.7.0',
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=[
        'pandas',
        'numpy',
        'statistics',
        'scikit-learn'
    ],
    entry_points={
        'console_scripts': [
            'estimate_start_times = main:main',
        ]
    }
)
