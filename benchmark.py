"""Simple benchmarks to compare performance of related calls."""

import argparse
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simple compression benchmark tests.")
    parser.add_argument(
        "-c",
        "--compression",
        nargs="+",
        help="Compression types to test. Available options depend on compression initialization checks.",
    )
    parser.add_argument(
        "-n",
        "--number",
        type=int,
        default=25_000,
        help="Number of iterations to run per test loop.",
    )
    parser.add_argument(
        "-r",
        "--repeat",
        type=int,
        default=1,
        help="Number of times to repeat the test loop.",
    )
    parser.add_argument(
        "-e",
        "--ezfs-only",
        action="store_true",
        help="Only run EZFS adapter tests. Do not run tests that use open() directly from compression modules.",
    )
    return parser.parse_args()


def main() -> None:
    """Run all the selected benchmarks."""
    args = _parse_args()
    number = args.number
    repeat = args.repeat

    filesystem = ezfs.LocalFilesystem(".")
    # filesystem = ezfs.MemFilesystem()
    # filesystem = ezfs.S3BotoFilesystem(
    #     "bucket-name",
    #     access_key_id="accesskey",
    #     secret_access_key="secretkey",
    # )
    # filesystem = ezfs.SQLiteFilesystem(f"{TEST_FILE_NAME}.db")
    # filesystem.create_table()

    compressors = ezfs.init_compressors()
    selected = args.compression or compressors

    print(f'{"All compression types:":<{COL_WIDTH}}', ", ".join(compressors))
    print(f'{"Selected types:":<{COL_WIDTH}}', ", ".join(selected) if selected != compressors else "all")
    print(f'{"Count:":<{COL_WIDTH}}', number)
    print(f'{"Repeat:":<{COL_WIDTH}}', repeat)
    print()

    disable_open = (
        "blosc",
        "brotli",
        "snappy",
    )
    for suffix in selected:
        compressor = ezfs.__COMPRESSORS__[suffix]
        if not compressor or compressor == ezfs.NO_COMPRESSION:
            continue
        tests = []
        if suffix not in disable_open and not args.ezfs_only:
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
