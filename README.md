
[![os: windows mac linux](https://img.shields.io/badge/os-linux_|_macos_|_windows-blue)](https://docs.python.org/3.10/)
[![python: 3.10+](https://img.shields.io/badge/python-3.10_|_3.11_|_3.12-blue)](https://devguide.python.org/versions)
[![python style: google](https://img.shields.io/badge/python%20style-google-blue)](https://google.github.io/styleguide/pyguide.html)
[![imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://github.com/PyCQA/isort)
[![code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![code style: pycodestyle](https://img.shields.io/badge/code%20style-pycodestyle-green)](https://github.com/PyCQA/pycodestyle)
[![doc style: pydocstyle](https://img.shields.io/badge/doc%20style-pydocstyle-green)](https://github.com/PyCQA/pydocstyle)
[![static typing: mypy](https://img.shields.io/badge/static_typing-mypy-green)](https://github.com/python/mypy)
[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/PyCQA/pylint)
[![testing: pytest](https://img.shields.io/badge/testing-pytest-yellowgreen)](https://github.com/pytest-dev/pytest)
[![security: bandit](https://img.shields.io/badge/security-bandit-black)](https://github.com/PyCQA/bandit)
[![license: MIT](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)


# EZFS

EZFS is an optimized, minimal dependency (down to 0), virtual filesystem adapter library for Python.
The library provides abstraction layers for reading and writing to various backend storage solutions
with a single frontend. Reading and writing is supported for text and binary files across local, remote,
and memory "filesystems". Additional compression types and storage types can be supported by extending
the primary `File`, `Filesystem`, and `Compressor` adapters.


## Table Of Contents

  * [Compatibility](#compatibility)
  * [Getting Started](#getting-started)
    * [Installation](#installation)
  * [How Tos](#how-tos)
    * [Write a file with compress](#write-a-file-with-compression)
    * [Read a file with compress](#read-a-file-with-compression)
    * [Access a file (object) in an S3 bucket, and use compression](#access-a-file-object-in-an-s3-bucket-and-use-compression)
  * [Why EZFS?](#why-ezfs)
    * [What does EZFS provide?](#what-does-ezfs-provide)
    * [What does EZFS not provide?](#what-does-ezfs-not-provide)
    * [Dependency Simplicity Example](#dependency-simplicity-example)
    * [Optimized Remote Filesystem Example](#optimized-remote-filesystem-example)
  * [Contributing](#contributing)


## Compatibility

- Supports Python 3.10+
- Supports multiple compression types
  - `bz2`, `gzip`, `lzma` (when built into Python)
  - `blosc`, `brotli`, `lz4`, `snappy`, and `zstd` (when installed separately)
- Support multiple storage types
  - `sqlite3` (when built into Python)
  - `S3` (when installed separately)
- Theoretically any compression type, or backend storage type, by extending `Compressor`, `File`, and `Filesystem` 


## Getting Started

### Installation

Install EZFS via pip:
```shell
pip install ezfs
```

Or via git clone:
```shell
git clone <path to fork>
cd ezfs
pip install .
```

Or build and install from wheel:
```shell
# Build locally.
git clone <path to fork>
cd ezfs
make wheel

# Push dist/ezfs*.tar.gz to environment where it will be installed.
pip install dist/ezfs*.tar.gz
```

Or via copy and paste (only a single file is required):
```shell
# Copy:
cp ezfs.py <target project directory>
```


## How Tos

EZFS filesystems and file objects are designed to work nearly identical to native `open()` file handles.
Basic read and write operations can be directly swapped out after creating a filesystem adapter, and calling `open()`
against the filesystem instead of Python built-ins, or 3rd party compression libraries. Here are a few examples
of how to use the more advanced features, such as compression and remote storage.

### Write a file with compression
```python
import ezfs

filesystem = ezfs.LocalFilesystem('/tmp')
with filesystem.open('test-file.txt.gz', 'w+', compression='gzip') as out_file:
    out_file.write('test message')

# Or automatically compress all files on the "filesystem" on write:
filesystem = ezfs.LocalFilesystem('/tmp', compression='gzip')
with filesystem.open('test-file.txt.gz', 'w+') as out_file:
    out_file.write('test message')
```

### Read a file with compression
```python
import ezfs

filesystem = ezfs.LocalFilesystem('/tmp')
with filesystem.open('test-file.txt.gz', compression='gzip') as in_file:
    print(in_file.read())

# Or automatically decompress all files on the "filesystem" on read:
filesystem = ezfs.LocalFilesystem('/tmp', compression='gzip')
with filesystem.open('test-file.txt.gz') as in_file:
    print(in_file.read())
```

### Access a file (object) in an S3 bucket, and use compression
```python
import ezfs

# To use advanced compression types, they must be installed separately.
filesystem = ezfs.S3BotoFilesystem(
    'my-bucket-1234',
    access_key_id='ABC123',
    secret_access_key='abcdefg1234567',
    compression='zstd',
)
with filesystem.open('test-file.txt.zst', 'w+') as out_file:
    out_file.write('test message')
with filesystem.open('test-file.txt.zst') as in_file:
    print(in_file.read())
```


## Why EZFS?

To simplify simple use cases.

EZFS is a very lightweight library (one file!), used to optimize simple use cases, or provide a starting point
for more complex use cases. While there are other libraries that can help accomplish filesystem-like use cases
depending on the backend, such as `s3fs` for S3, they may be more than needed or wanted. For example, perhaps
you have predictable logic to store/read files, and don't need to browse the filesystem tree. Perhaps you want
to leverage a custom service to act as storage interchangeably with local files, without installing extra
dependencies from other solutions. EZFS adapters can help with that. If you need full metadata support like
filesystem tree browsing, or file permissions, EZFS cannot help with that (natively), and recommends using a
more feature rich solution, or extending the adapters to fit your needs.

### What does EZFS provide?

EZFS provides a shared, optimized, interface to read and write files to various backend locations,
with or without compression. The backend for the storage can often be changed with a single line,
without changing the rest of the code.

### What does EZFS not provide?

EZFS does not provide a complex feature set for advanced use cases, such as managing permissions or other metadata
on filesystems. EZFS also not provide streaming interfaces for processing larger than memory files in "chunks".

### Dependency Simplicity Example

Here is an example of using a library such as `sf3s` vs `ezfs` for basic read and write to S3, and its effect
on required dependencies in a project. A basic `boto3` install (only requirement for `ezfs` support) will add
the following to the environment:
- boto3
- botocore
- jmespath
- python-dateutil
- s3transfer
- six
- urllib3

An `s3fs` install will add the following in addition to the core `boto3` requirements:
- aiobotocore
- aiohttp
- aioitertools
- aiosignal
- async-timeout
- attrs
- idna
- frozenlist
- fsspec
- multidict
- s3fs
- wrapt
- yarl

Perhaps you already have all these requirements. Great! Then S3FS may be a better fit. Perhaps you don't have these,
and want to reduce requirements that may add maintenance overhead to resolve security vulnerabilities. Great!
EZFS may be a better fit. Still not sure? Continue reading for a performance example.

### Optimized Remote Filesystem Example

Here is a basic performance example, using S3 + `pandas` to store DataFrames. By using a Filesystem adapter
to store the results, it can optimize the S3 client used to reduce networking overhead. The optimization benefit
is greater with small files, but even larger files benefit, and the simplicity to use stays the same.
- Small file: 100K
- Large file: 1M
- 100 iterations per test

| Scenario               | Write    | Read        |
|------------------------|----------|-------------|
| pandas s3fs small raw  | 25 sec   | 15.6 sec    |
| pandas ezfs small raw  | 16.9 sec | 8.39 sec    |
| pandas s3fs large raw  | 47.3 sec | 17.9 sec    |
| pandas ezfs large raw  | 28.7 sec | 11.2 sec    |
| pandas s3fs small zstd | 20.2 sec | 12 sec      |
| pandas ezfs small zstd | 12.6 sec | 6.76 sec    |
| pandas s3fs large zstd | 37.4 sec | 18.1 sec    |
| pandas ezfs large zstd | 21.5 sec | 11.1 sec    |


### Contributing

EZFS is not currently accepting new features. Minor features may be added to improve the native use cases,
but outside minor changes it will only receive bug fixes and dependency updates. This decision is to ensure
EZFS will stay focused on its primary goal: stay simple and efficient, by focusing on simple use cases.
Feel free to import, fork, copy, etc., to other projects to expand the scope of its ecosystem. Refer to the
[Contributing Guide](CONTRIBUTING.md) for information on how to contribute to this project.
