from setuptools import setup

setup(
    name='estimate_start_times',
    version='1.4.0',
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=[
        'pandas',
        'numpy',
        'statistics',
        'scikit-learn'
    ]
)
