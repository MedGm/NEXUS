from setuptools import setup, find_packages

setup(
    name="nexus-common",
    version="0.1.0",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "confluent-kafka>=2.4.0",
        "fastavro>=1.9.0",
        "certifi",
        "httpx",
        "authlib",
        "cachetools",
        "attrs",
        "sniffio",
    ],
)
