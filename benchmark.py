"""Simple benchmarks to compare performance of related calls."""

import gzip
import timeit
from typing import Callable

import zstandard

import ezfs

TEST_STRING = '"Test string content for storage in file that is 64 bytes long."'

tfs = ezfs.LocalFilesystem(".")
# tfs = ezfs.MemFilesystem()
# tfs = ezfs.S3BotoFilesystem(
#     'bucket-name',
#     access_key_id='accesskey',
#     secret_access_key='secretkey',
# )


def _bench_all(number: int = 1, repeat: int = 1) -> None:
    """Run all the selected benchmarks."""
    print(f'{"Directory:":<23}', getattr(tfs, "directory", "None"))
    print(f'{"Compression types:":<23}', ", ".join(ezfs.init_compressors()))
    print(f'{"Count:":<23}', number)
    print(f'{"Repeat:":<23}', repeat)
    for func in funcs:
        _bench_func(func, number, repeat)


def _bench_func(func: Callable, number: int, repeat: int) -> None:
    result = timeit.repeat(func, number=number, repeat=repeat)
    for duration in result:
        print(f"{func.__name__.replace('_bench_', ''):<23}", _format_time(duration))


def _bench_gzip_read_binary() -> bytes:
    with gzip.open("test.gz", "rb") as file:
        return file.read()


def _bench_gzip_read_text() -> str:
    with gzip.open("test.gz", "rt") as file:
        return file.read()


def _bench_gzip_read_binary_ezfs() -> bytes:
    with tfs.open("test.gz", "rb", compression="gzip") as file:
        return file.read()


def _bench_gzip_read_text_ezfs() -> str:
    with tfs.open("test.gz", "rt", compression="gzip") as file:
        return file.read()


def _bench_zstd_read_binary() -> bytes:
    with zstandard.open("test.zst", "rb") as file:
        return file.read()


def _bench_zstd_read_text() -> str:
    with zstandard.open("test.zst", "rt") as file:
        return file.read()


def _bench_zstd_read_binary_ezfs() -> bytes:
    with tfs.open("test.zst", "rb", compression="zstd") as file:
        return file.read()


def _bench_zstd_read_text_ezfs() -> str:
    with tfs.open("test.zst", "rt", compression="zstd") as file:
        return file.read()


def _bench_gzip_write_binary() -> int:
    with gzip.open("test.gz", "wb+") as file:
        return file.write(TEST_STRING.encode("utf-8"))


def _bench_gzip_write_text() -> int:
    with gzip.open("test.gz", "wt+") as file:
        return file.write(TEST_STRING)


def _bench_gzip_write_binary_ezfs() -> int:
    with tfs.open("test.gz", "wb+", compression="gzip") as file:
        return file.write(TEST_STRING)


def _bench_gzip_write_text_ezfs() -> int:
    with tfs.open("test.gz", "wt+", compression="gzip") as file:
        return file.write(TEST_STRING)


def _bench_zstd_write_binary() -> int:
    with zstandard.open("test.zst", "wb") as file:
        return file.write(TEST_STRING.encode("utf-8"))


def _bench_zstd_write_text() -> int:
    with zstandard.open("test.zst", "wt") as file:
        return file.write(TEST_STRING)


def _bench_zstd_write_binary_ezfs() -> int:
    with tfs.open("test.zst", "wb+", compression="zstd") as file:
        return file.write(TEST_STRING)


def _bench_zstd_write_text_ezfs() -> int:
    with tfs.open("test.zst", "wt+", compression="zstd") as file:
        return file.write(TEST_STRING)


def _format_time(duration: float) -> str:
    units = {
        "nsec": 1e-9,
        "usec": 1e-6,
        "msec": 1e-3,
        "sec": 1.0,
    }
    scales = [(scale, unit) for unit, scale in units.items()]
    scales.sort(reverse=True)
    for scale, unit in scales:
        if duration >= scale:
            return "%.*g %s" % (3, duration / scale, unit)


funcs = [
    # Write tests.
    _bench_gzip_write_binary,
    _bench_gzip_write_text,
    _bench_gzip_write_binary_ezfs,
    _bench_gzip_write_text_ezfs,
    _bench_zstd_write_binary,
    _bench_zstd_write_text,
    _bench_zstd_write_binary_ezfs,
    _bench_zstd_write_text_ezfs,
    # Read tests.
    _bench_gzip_read_binary,
    _bench_gzip_read_text,
    _bench_gzip_read_binary_ezfs,
    _bench_gzip_read_text_ezfs,
    _bench_zstd_read_binary,
    _bench_zstd_read_text,
    _bench_zstd_read_binary_ezfs,
    _bench_zstd_read_text_ezfs,
]

if __name__ == "__main__":
    _bench_all(10000)
