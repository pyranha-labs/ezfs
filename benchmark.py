"""Simple benchmarks to compare performance of related calls."""

import timeit
from types import ModuleType
from typing import Callable
from typing import Iterable

import ezfs

TEST_FILE_NAME = "compression.test"
TEST_STRING = '"Test string content for storage in file that is 64 bytes long."'
TEST_STRING_BINARY = TEST_STRING.encode("utf-8")
COL_WIDTH = 24


def _bench_all(tests: list[tuple], number: int = 1, repeat: int = 1) -> None:
    for func, args, suffix in tests:
        _bench_func(func, args, number, repeat, test_suffix=suffix)


def _bench_func(func: Callable, args: Iterable, number: int, repeat: int, test_suffix: str = "") -> None:
    result = timeit.repeat(lambda: func(*args), number=number, repeat=repeat)
    for duration in result:
        print(f"{func.__name__.replace('_bench_', '') + test_suffix:<{COL_WIDTH}}", _format_time(duration))


def _bench_ezfs_read(filesystem: ezfs.Filesystem, mode: str, compression: str) -> bytes:
    with filesystem.open(TEST_FILE_NAME, mode, compression=compression) as file:
        return file.read()


def _bench_ezfs_write(filesystem: ezfs.Filesystem, mode: str, compression: str, content: str | bytes) -> int:
    with filesystem.open(TEST_FILE_NAME, mode, compression=compression) as file:
        return file.write(content)


def _bench_native_read(opener: ModuleType, mode: str) -> bytes:
    with opener.open(TEST_FILE_NAME, mode) as file:
        return file.read()


def _bench_native_write(opener: ModuleType, mode: str, content: str | bytes) -> int:
    with opener.open(TEST_FILE_NAME, mode) as file:
        return file.write(content)


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


def main() -> None:
    """Run all the selected benchmarks."""
    filesystem = ezfs.LocalFilesystem(".")
    # filesystem = ezfs.MemFilesystem()
    # filesystem = ezfs.S3BotoFilesystem(
    #     "bucket-name",
    #     access_key_id="accesskey",
    #     secret_access_key="secretkey",
    # )
    # filesystem = ezfs.SQLiteFilesystem(f"{TEST_FILE_NAME}.db")
    # filesystem.create_table()

    number = 25000
    repeat = 1
    print(f'{"Compression types:":<{COL_WIDTH}}', ", ".join(ezfs.init_compressors()))
    print(f'{"Count:":<{COL_WIDTH}}', number)
    print(f'{"Repeat:":<{COL_WIDTH}}', repeat)
    compressors = ezfs.__COMPRESSORS__
    test_suites = [
        (compressors.get("bz2"), "bz2"),
        (compressors.get("gzip"), "gzip"),
        (compressors.get("lzma"), "lzma"),
        (compressors.get("blosc"), "blosc", False),
        (compressors.get("brotli"), "brotli", False),
        (compressors.get("lz4"), "lz4"),
        (compressors.get("snappy"), "snappy", False),
        (compressors.get("zstd"), "zstd"),
    ]
    for test_suite in test_suites:
        compressor = test_suite[0]
        if not compressor:
            continue
        suffix = test_suite[1]
        native_open_tests = test_suite[2] if len(test_suite) > 2 else True
        tests = []
        if native_open_tests:
            tests.extend(
                [
                    (_bench_native_write, (compressor, "wb", TEST_STRING_BINARY), f"_{suffix}_binary"),
                    (_bench_native_write, (compressor, "wt", TEST_STRING), f"_{suffix}_text"),
                    (_bench_native_read, (compressor, "rb"), f"_{suffix}_binary"),
                    (_bench_native_read, (compressor, "rt"), f"_{suffix}_text"),
                ]
            )
        tests.extend(
            [
                (_bench_ezfs_write, (filesystem, "wb", suffix, TEST_STRING_BINARY), f"_{suffix}_binary"),
                (_bench_ezfs_write, (filesystem, "wt", suffix, TEST_STRING), f"_{suffix}_text"),
                (_bench_ezfs_read, (filesystem, "rb", suffix), f"_{suffix}_binary"),
                (_bench_ezfs_read, (filesystem, "rt", suffix), f"_{suffix}_text"),
            ]
        )
        _bench_all(tests, number, repeat)


if __name__ == "__main__":
    main()
