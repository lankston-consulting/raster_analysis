from setuptools import setup, find_namespace_packages
from Cython.Build import cythonize

setup(
    name="RasterAnalysis",
    version="0.1",
    packages=find_namespace_packages(),
    ext_modules=cythonize("ra/*.pyx"),
)
