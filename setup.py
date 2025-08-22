from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk>=0.51.0",
    "requests>=2.28.0",
    "python-dateutil>=2.8.0",
]

TEST_REQUIREMENTS = [
    "pytest>=6.2.5",
    "pytest-mock>=3.6.1",
    "requests-mock>=1.9.3",
]

setup(
    name="source-btg",
    version="0.1.0",
    description="Source implementation for BTG Pactual API",
    author="Data Team",
    author_email="data-team@empresa.com",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
    package_data={
        "": ["*.json", "*.yaml", "*.yml"],
    },

    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License", 
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)