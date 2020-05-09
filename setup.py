from setuptools import setup, find_packages

with open("README.md", "r") as readme_file:
    readme = readme_file.read()

requirements = ["twine==1.13.0",
                "pika==1.1.0",
                "minio==5.0.10",
                "redis==3.4.1",
                "pandas==1.0.1",
                "pyarrow==0.17.0"]

setup(
    name="general_utils",
    version="0.0.4.1",
    author="Kashyap Madariyil",
    author_email="kashyapmadariyil@gmail.com",
    description="A general package that has useful functionalities",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/kashy750/python_package",
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    ],
)