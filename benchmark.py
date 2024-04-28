#! /usr/bin/env python3

"""Simple benchmarks to compare performance of related calls."""

import argparse
import timeit
from types import ModuleType
from typing import Callable
from typing import Iterable

import ezfs

COL_WIDTH = 34

TEST_FILE_NAME = "compression.test"
TEST_STRING = '"Test string content for storage in file that is 64 bytes long."'
TEST_STRING_BINARY = TEST_STRING.encode("utf-8")


def _bench_all(tests: list[tuple], number: int = 1, repeat: int = 1) -> None:
    for func, args, suffix in tests:
        _bench_func(func, args, number, repeat, test_suffix=suffix)


def _bench_func(
    func: Callable,
    args: Iterable,
    number: int,
    repeat: int,
    test_suffix: str = "",
) -> None:
    test_suffix = f"_{test_suffix}" if test_suffix else ""
    result = timeit.repeat(lambda: func(*args), number=number, repeat=repeat)
    for duration in result:
        print(f"{func.__name__.replace('_bench_', '') + test_suffix:<{COL_WIDTH}}", _format_time(duration))


def _bench_ezfs_filesystem(
    filesystem: ezfs.Filesystem,
    fs_type: str,
    compression_types: Iterable[str],
    number: int,
    repeat: int,
) -> None:
    for compression in compression_types:
        compressor = ezfs.__COMPRESSORS__[compression]
        if not compressor or compressor == ezfs.NO_COMPRESSION:
            continue
        tests = [
            (_bench_ezfs_write, (filesystem, "wb", compression, TEST_STRING_BINARY), f"{fs_type}_{compression}_binary"),
            (_bench_ezfs_write, (filesystem, "wt", compression, TEST_STRING), f"{fs_type}_{compression}_text"),
            (_bench_ezfs_read, (filesystem, "rb", compression), f"{fs_type}_{compression}_binary"),
            (_bench_ezfs_read, (filesystem, "rt", compression), f"{fs_type}_{compression}_text"),
        ]
        _bench_all(tests, number, repeat)


def _bench_ezfs_read(filesystem: ezfs.Filesystem, mode: str, compression: str) -> bytes:
    with filesystem.open(TEST_FILE_NAME, mode, compression=compression) as file:
        return file.read()


def _bench_ezfs_write(filesystem: ezfs.Filesystem, mode: str, compression: str, content: str | bytes) -> int:
    with filesystem.open(TEST_FILE_NAME, mode, compression=compression) as file:
        return file.write(content)


def _bench_native_filesystem(
    compression_types: Iterable[str],
    number: int,
    repeat: int,
) -> None:
    no_open = (
        "blosc",
        "brotli",
        "snappy",
    )
    for compression in compression_types:
        compressor = ezfs.__COMPRESSORS__[compression]
        if not compressor or compressor == ezfs.NO_COMPRESSION:
            continue
        if compression in no_open:
            tests = [
                (_bench_native_write_manual, (compressor, "wb", TEST_STRING_BINARY), f"{compression}_binary"),
                (_bench_native_write_manual, (compressor, "wt", TEST_STRING), f"{compression}_text"),
                (_bench_native_read_manual, (compressor, "rb"), f"{compression}_binary"),
                (_bench_native_read_manual, (compressor, "rt"), f"{compression}_text"),
            ]
        else:
            tests = [
                (_bench_native_write, (compressor, "wb", TEST_STRING_BINARY), f"{compression}_binary"),
                (_bench_native_write, (compressor, "wt", TEST_STRING), f"{compression}_text"),
                (_bench_native_read, (compressor, "rb"), f"{compression}_binary"),
                (_bench_native_read, (compressor, "rt"), f"{compression}_text"),
            ]
        _bench_all(tests, number, repeat)


def _bench_native_read(opener: ModuleType, mode: str) -> bytes:
    with opener.open(TEST_FILE_NAME, mode) as file:
        return file.read()


def _bench_native_read_manual(compressor: ModuleType, mode: str) -> bytes:
    # Alternative version of _bench_native_read with manual file open and read, due to compressor missing open alias.
    with open(TEST_FILE_NAME, 'rb') as file:
        data = compressor.decompress(file.read())
        if 't' in mode:
            data = data.decode('utf-8')
        return data


def _bench_native_write(opener: ModuleType, mode: str, content: str | bytes) -> int:
    with opener.open(TEST_FILE_NAME, mode) as file:
        return file.write(content)


def _bench_native_write_manual(compressor: ModuleType, mode: str, content: str | bytes) -> int:
    # Alternative version of _bench_native_write with manual file open and write, due to compressor missing open alias.
    with open(TEST_FILE_NAME, 'wb') as file:
        if "t" in mode:
            content = content.encode('utf-8')
        return file.write(compressor.compress(content))


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
        help="Compression type(s) to test. Available options depend on compression initialization checks.",
    )
    parser.add_argument(
        "-f",
        "--filesystem",
        nargs="+",
        choices=["native", "local", "memory", "sqlite"],
        help="Filesystem type(s) to test.",
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
        "-m",
        "--memory",
        action="store_true",
        help="Use in-memory storge instead of local for filesystems that support both. Default uses local storage.",
    )
    return parser.parse_args()


def main() -> None:
    """Run all the selected benchmarks."""
    args = _parse_args()
    number = args.number
    repeat = args.repeat
    fs_types = args.filesystem or [
        "native",
        "local",
    ]

    compressors = ezfs.init_compressors()
    selected = args.compression or compressors

    print(f'{"Available compression types:":<{COL_WIDTH}}', ", ".join(compressors))
    print(f'{"Selected compression types:":<{COL_WIDTH}}', ", ".join(selected) if selected != compressors else "all")
    print(f'{"Selected filesystem types:":<{COL_WIDTH}}', ", ".join(fs_types))
    print(f'{"Test iterations per loop:":<{COL_WIDTH}}', number)
    print(f'{"Test loops:":<{COL_WIDTH}}', repeat)
    print()

    if "native" in fs_types:
        fs_types.remove("native")
        _bench_native_filesystem(selected, number, repeat)

    for fs_type in fs_types:
        filesystem = None
        if fs_type == "local":
            filesystem = ezfs.LocalFilesystem(".")
        elif fs_type == "memory":
            filesystem = ezfs.MemFilesystem()
        elif fs_type == "sqlite":
            filesystem = ezfs.SQLiteFilesystem(":memory:" if args.memory else f"{TEST_FILE_NAME}.db")
            import sqlite3

            try:
                filesystem.create_table()
            except sqlite3.OperationalError as error:
                if "already exists" not in str(error):
                    raise
        # filesystem = ezfs.S3BotoFilesystem(
        #     "bucket-name",
        #     access_key_id="accesskey",
        #     secret_access_key="secretkey",
        # )
        if filesystem:
            _bench_ezfs_filesystem(filesystem, fs_type, selected, number, repeat)


if __name__ == "__main__":
    main()
