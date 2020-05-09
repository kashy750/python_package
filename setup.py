import os
from setuptools import setup, find_packages
from pip._internal.req import parse_requirements

dirname = os.path.dirname(__file__)
requirements_path = os.path.join(dirname, "general_utils/requirements.txt")
readme_path = os.path.join(dirname, "README.md")


with open(readme_path, "r") as readme_file:
    readme = readme_file.read()



# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements(requirements_path, session=False)

reqs = [str(ir.req) for ir in install_reqs]

# requirements = ["twine==1.13.0",
#                 "pika==1.1.0",
#                 "minio==5.0.10",
#                 "redis==3.4.1",
#                 "pandas==1.0.1",
#                 "pyarrow==0.17.0"]

setup(
    name="general_utils",
    version="0.0.4.2",
    author="Kashyap Madariyil",
    author_email="kashyapmadariyil@gmail.com",
    description="A general package that has useful functionalities",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/kashy750/python_package",
    packages=find_packages(),
    install_requires=reqs,
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    ],
)