from setuptools import setup, find_packages

VERSION = "0.0.1"
DESCRIPTION = "The service package for data crawling processes"
# REQUIREMENTS = [i.strip() for i in open("D:\\EdgeProp\\repos\\edgeprop-analytics\\ep-services\\requirements.txt").readlines()]

setup(
    name="ep_services",
    version=VERSION,
    author="Quoc Nguyen",
    author_email="nguyenaiquoc.cntt@gmail.com",
    license='MIT',
    description=DESCRIPTION,
    packages=find_packages(),
    install_requires=['pandas>=1.1.5', 'boto3>=1.17.98'],  # REQUIREMENTS
    python_requires='>=3'
)

'''
*** distributing ep_services ****
build:
python -m pip install --upgrade build
python -m build --outdir ./ep_services/package-dist

install package:
python -m pip install --no-index --find-links="/home/edge/python-crawler/service-packages/" ep_services
python3 -m pip install --no-index --find-links="D:/EdgeProp/repos/edgeprop-analytics/python-crawler/service-packages/" ep_services
'''