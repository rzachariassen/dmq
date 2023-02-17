from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

ext_modules=[ Extension("dmq", sources=["pydmq.pyx", "dmq.c"]) ]

setup(
  name = "dmq",
  version = "1.0",
  author = "Rayan Zachariassen",
  author_email = "rayan@ecasa.net",
  ext_modules = cythonize(ext_modules)
)
