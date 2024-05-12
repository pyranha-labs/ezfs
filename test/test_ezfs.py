"""Unit tests for EZFS utilities."""

import io
import sqlite3
import tempfile
from typing import Any
from typing import Callable

import pytest
import zstandard

import ezfs

TEST_FILE = "test.txt"
TEST_STRING = "Test string content for storage repeated 3 times." * 3
TEST_STRING_BINARY = TEST_STRING.encode("utf-8")
SWAP_TRANSFORM_1 = ezfs.Transform(
    apply=lambda data: data.decode().replace("e", "-").encode(),
    remove=lambda data: data.decode().replace("-", "e").encode(),
)
SWAP_TRANSFORM_2 = ezfs.Transform(
    apply=lambda data: data.decode().replace("t", "*").encode(),
    remove=lambda data: data.decode().replace("*", "t").encode(),
)


TEST_CASES = {
    "filesystem": {
        "invalid mode": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "u",
                },
            },
            "raises": ValueError,
        },
        "read and write mode": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "rw",
                },
            },
            "raises": ValueError,
        },
        "text and binary mode": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "tb",
                },
            },
            "raises": ValueError,
        },
        "not found": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE + "abc",
                },
            },
            "raises": FileNotFoundError,
        },
        "not writeable": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                },
                "content": TEST_STRING,
            },
            "raises": io.UnsupportedOperation,
        },
        "not readable": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "wb",
                },
            },
            "raises": io.UnsupportedOperation,
        },
        "not bytes": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "wb",
                },
                "content": TEST_STRING,
            },
            "raises": TypeError,
        },
        "not str": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "wt",
                },
                "content": TEST_STRING_BINARY,
            },
            "raises": TypeError,
        },
        "not bytes or string": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "wt",
                },
                "content": ("test", b"test"),
            },
            "raises": TypeError,
        },
        "no compression or transform": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "w",
                },
                "content": TEST_STRING,
            },
            "returns": {
                "wrote": 147,
                "raw": TEST_STRING_BINARY,
                "read_bytes": TEST_STRING_BINARY,
                "read_text": TEST_STRING,
            },
        },
        "compression at filesystem level": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "filesystem_kwargs": {
                    "compression": "zstd",
                },
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "w",
                },
                "content": TEST_STRING,
            },
            "returns": {
                "wrote": 66,
                "raw": b"(\xb5/\xfd \x93\xcd\x01\x00\x14\x03Test string content for storage repeated 3 times.\x01\x00\xf1tRP",
                "read_bytes": TEST_STRING_BINARY,
                "read_text": TEST_STRING,
            },
        },
        "compression at file level": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "w",
                    "compression": "zstd",
                },
                "content": TEST_STRING,
            },
            "returns": {
                "wrote": 66,
                "raw": b"(\xb5/\xfd \x93\xcd\x01\x00\x14\x03Test string content for storage repeated 3 times.\x01\x00\xf1tRP",
                "read_bytes": TEST_STRING_BINARY,
                "read_text": TEST_STRING,
            },
        },
        "transform at filesystem level": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "filesystem_kwargs": {
                    "transform": SWAP_TRANSFORM_1,
                },
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "w",
                },
                "content": TEST_STRING[:49],
            },
            "returns": {
                "wrote": 49,
                "raw": b"T-st string cont-nt for storag- r-p-at-d 3 tim-s.",
                "read_bytes": TEST_STRING_BINARY[:49],
                "read_text": TEST_STRING[:49],
            },
        },
        "transform at file level": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "w",
                    "transform": SWAP_TRANSFORM_1,
                },
                "content": TEST_STRING[:49],
            },
            "returns": {
                "wrote": 49,
                "raw": b"T-st string cont-nt for storag- r-p-at-d 3 tim-s.",
                "read_bytes": TEST_STRING_BINARY[:49],
                "read_text": TEST_STRING[:49],
            },
        },
        "chained transform": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "w",
                    "transform": ezfs.Transform.chain(SWAP_TRANSFORM_1, SWAP_TRANSFORM_2),
                },
                "content": TEST_STRING[:49],
            },
            "returns": {
                "wrote": 49,
                "raw": b"T-s* s*ring con*-n* for s*orag- r-p-a*-d 3 *im-s.",
                "read_bytes": TEST_STRING_BINARY[:49],
                "read_text": TEST_STRING[:49],
            },
        },
        "local str": {
            "kwargs": {
                "filesystem_cls": ezfs.LocalFilesystem,
                "filesystem_kwargs": {
                    "directory": "replaced_by_test_with_tmpdir",
                },
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "w",
                },
                "content": TEST_STRING,
            },
            "returns": {
                "wrote": 147,
                "raw": TEST_STRING_BINARY,
                "read_bytes": TEST_STRING_BINARY,
                "read_text": TEST_STRING,
            },
        },
        "local binary": {
            "kwargs": {
                "filesystem_cls": ezfs.LocalFilesystem,
                "filesystem_kwargs": {
                    "directory": "replaced_by_test_with_tmpdir",
                },
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "wb",
                },
                "content": TEST_STRING_BINARY,
            },
            "returns": {
                "wrote": 147,
                "raw": TEST_STRING_BINARY,
                "read_bytes": TEST_STRING_BINARY,
                "read_text": TEST_STRING,
            },
        },
        "local text with compression and coercion to bytes": {
            "kwargs": {
                "filesystem_cls": ezfs.LocalFilesystem,
                "filesystem_kwargs": {
                    "directory": "replaced_by_test_with_tmpdir",
                },
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "wt",
                    "compression": "zstd",
                },
                "content": TEST_STRING,
            },
            "returns": {
                "wrote": 66,
                "raw": b"(\xb5/\xfd \x93\xcd\x01\x00\x14\x03Test string content for storage repeated 3 times.\x01\x00\xf1tRP",
                "read_bytes": TEST_STRING_BINARY,
                "read_text": TEST_STRING,
            },
        },
        "local unsafe path": {
            "kwargs": {
                "filesystem_cls": ezfs.LocalFilesystem,
                "filesystem_kwargs": {
                    "directory": "replaced_by_test_with_tmpdir",
                },
                "file_kwargs": {
                    "file": "../" + TEST_FILE,
                    "mode": "w",
                },
            },
            "raises": FileNotFoundError,
        },
        "sqlite": {
            "kwargs": {
                "filesystem_cls": ezfs.SQLiteFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "w",
                },
                "content": TEST_STRING,
            },
            "returns": {
                "wrote": 147,
                "raw": TEST_STRING_BINARY,
                "read_bytes": TEST_STRING_BINARY,
                "read_text": TEST_STRING,
            },
        },
        "sqlite binary": {
            "kwargs": {
                "filesystem_cls": ezfs.SQLiteFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "wb",
                },
                "content": TEST_STRING_BINARY,
            },
            "returns": {
                "wrote": 147,
                "raw": TEST_STRING_BINARY,
                "read_bytes": TEST_STRING_BINARY,
                "read_text": TEST_STRING,
            },
        },
        "sqlite invalid table": {
            "kwargs": {
                "filesystem_cls": ezfs.SQLiteFilesystem,
                "filesystem_kwargs": {
                    "table_name": "inv@lid",
                },
            },
            "raises": ValueError,
        },
        "sqlite invalid file col": {
            "kwargs": {
                "filesystem_cls": ezfs.SQLiteFilesystem,
                "filesystem_kwargs": {
                    "file_col": "inv@lid",
                },
            },
            "raises": ValueError,
        },
        "sqlite invalid content col": {
            "kwargs": {
                "filesystem_cls": ezfs.SQLiteFilesystem,
                "filesystem_kwargs": {
                    "content_col": "inv@lid",
                },
            },
            "raises": ValueError,
        },
        "custom compressor with compression kwargs": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "filesystem_kwargs": {
                    "compression": ezfs.Compressor(zstandard, compress_kwargs={"level": 20}),
                },
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "w",
                },
                "content": TEST_STRING,
            },
            "returns": {
                "wrote": 57,
                "raw": b"(\xb5/\xfd \x93\x85\x01\x00\x12\xc3\t\x0e\xc0\xeb\xc2\x89\xd8\x8al6\xbf\x7f\xaa\xdauO \x8d\xce\x1bJ\xfcr2\xd6\xa6\xf0\x9e\xc5+\xbd\xf9v\xa3\xc6\x0e>\xfb\xb4\x08\x01\x00\xf1tRP",
                "read_bytes": TEST_STRING_BINARY,
                "read_text": TEST_STRING,
            },
        },
    },
    "filesystem remove": {
        "memory": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": "remove" + TEST_FILE,
                },
            },
            "returns": True,
        },
        "memory, not found": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": "remove" + TEST_FILE + "abc",
                },
                "skip_write": True,
            },
            "raises": FileNotFoundError,
        },
        "local": {
            "kwargs": {
                "filesystem_cls": ezfs.LocalFilesystem,
                "filesystem_kwargs": {
                    "directory": "replaced_by_test_with_tmpdir",
                },
                "file_kwargs": {
                    "file": "remove" + TEST_FILE,
                },
            },
            "returns": True,
        },
        "local, not found": {
            "kwargs": {
                "filesystem_cls": ezfs.LocalFilesystem,
                "filesystem_kwargs": {
                    "directory": "replaced_by_test_with_tmpdir",
                },
                "file_kwargs": {
                    "file": "remove" + TEST_FILE + "abc",
                },
                "skip_write": True,
            },
            "raises": FileNotFoundError,
        },
        "local, unsafe path": {
            "kwargs": {
                "filesystem_cls": ezfs.LocalFilesystem,
                "filesystem_kwargs": {
                    "directory": "replaced_by_test_with_tmpdir",
                },
                "file_kwargs": {
                    "file": "../remove" + TEST_FILE,
                },
                "skip_write": True,
            },
            "raises": FileNotFoundError,
        },
        "local, safe_paths disabled and dir": {
            "kwargs": {
                "filesystem_cls": ezfs.LocalFilesystem,
                "filesystem_kwargs": {
                    "directory": "replaced_by_test_with_tmpdir",
                    "safe_paths": False,
                },
                "file_kwargs": {
                    "file": "remove" + TEST_FILE,
                },
                "remove_kwargs": {
                    "name": "test",
                },
            },
            "raises": OSError,
        },
        "sqlite": {
            "kwargs": {
                "filesystem_cls": ezfs.SQLiteFilesystem,
                "file_kwargs": {
                    "file": "remove" + TEST_FILE,
                },
            },
            "returns": True,
        },
        "sqlite not found": {
            "kwargs": {
                "filesystem_cls": ezfs.SQLiteFilesystem,
                "file_kwargs": {
                    "file": "remove" + TEST_FILE + "abc",
                },
            },
            "returns": True,
        },
        "unsupported dir_fd": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": "remove" + TEST_FILE,
                },
                "remove_kwargs": {
                    "dir_fd": 1,
                },
            },
            "raises": NotImplementedError,
        },
    },
    "filesystem rename": {
        "memory": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "src": "rename" + TEST_FILE,
                "dst": "rename" + TEST_FILE + ".moved",
            },
            "returns": True,
        },
        "memory, not found": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "src": "rename" + TEST_FILE,
                "dst": "rename" + TEST_FILE + ".moved",
                "skip_write": True,
            },
            "raises": FileNotFoundError,
        },
        "local": {
            "kwargs": {
                "filesystem_cls": ezfs.LocalFilesystem,
                "src": "rename" + TEST_FILE,
                "dst": "rename" + TEST_FILE + ".moved",
                "filesystem_kwargs": {
                    "directory": "replaced_by_test_with_tmpdir",
                },
            },
            "returns": True,
        },
        "local, not found": {
            "kwargs": {
                "filesystem_cls": ezfs.LocalFilesystem,
                "src": "rename" + TEST_FILE,
                "dst": "rename" + TEST_FILE + ".moved",
                "filesystem_kwargs": {
                    "directory": "replaced_by_test_with_tmpdir",
                },
                "skip_write": True,
            },
            "raises": FileNotFoundError,
        },
        "local, unsafe src": {
            "kwargs": {
                "filesystem_cls": ezfs.LocalFilesystem,
                "src": "../rename" + TEST_FILE,
                "dst": "rename" + TEST_FILE + ".moved",
                "filesystem_kwargs": {
                    "directory": "replaced_by_test_with_tmpdir",
                },
                "skip_write": True,
            },
            "raises": FileNotFoundError,
        },
        "local, unsafe dst": {
            "kwargs": {
                "filesystem_cls": ezfs.LocalFilesystem,
                "src": "../rename" + TEST_FILE,
                "dst": "rename" + TEST_FILE + ".moved",
                "filesystem_kwargs": {
                    "directory": "replaced_by_test_with_tmpdir",
                },
                "skip_write": True,
            },
            "raises": FileNotFoundError,
        },
        "sqlite": {
            "kwargs": {
                "filesystem_cls": ezfs.SQLiteFilesystem,
                "src": "rename" + TEST_FILE,
                "dst": "rename" + TEST_FILE + ".moved",
            },
            "returns": True,
        },
        "sqlite not found": {
            "kwargs": {
                "filesystem_cls": ezfs.SQLiteFilesystem,
                "src": "rename" + TEST_FILE,
                "dst": "rename" + TEST_FILE + ".moved",
            },
            "returns": True,
        },
        "unsupported src_dir_fd": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "src": "rename" + TEST_FILE,
                "dst": "rename" + TEST_FILE + ".moved",
                "rename_kwargs": {
                    "src_dir_fd": 1,
                },
            },
            "raises": NotImplementedError,
        },
        "unsupported dst_dir_fd": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "src": "rename" + TEST_FILE,
                "dst": "rename" + TEST_FILE + ".moved",
                "rename_kwargs": {
                    "dst_dir_fd": 1,
                },
            },
            "raises": NotImplementedError,
        },
        "already exists": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "src": "rename" + TEST_FILE,
                "dst": "rename" + TEST_FILE,
            },
            "raises": FileExistsError,
        },
    },
    "file properties": {
        "memory": {
            "kwargs": {
                "filesystem_cls": ezfs.MemFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "w",
                },
            },
            "returns": {
                "str": "test.txt",
                "repr": "memfile:test.txt",
            },
        },
        "local": {
            "kwargs": {
                "filesystem_cls": ezfs.LocalFilesystem,
                "filesystem_kwargs": {
                    "directory": "replaced_by_test_with_tmpdir",
                },
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "w",
                },
            },
            "returns": {
                "str": "/test.txt",
                "repr": "localfile:PYTEST_TMP_DIR/test.txt",
            },
        },
        "sqlite": {
            "kwargs": {
                "filesystem_cls": ezfs.SQLiteFilesystem,
                "file_kwargs": {
                    "file": TEST_FILE,
                    "mode": "w",
                },
            },
            "returns": {
                "str": "test.txt",
                "repr": "sqlite3://:memory:?table_name=files&file=test.txt",
            },
        },
    },
}


def _tmpdir_wrapper(
    func: Callable,
    filesystem_cls: type[ezfs.Filesystem],
    *args: Any,
    filesystem_kwargs: dict | None = None,
    **kwargs: Any,
) -> bool:
    """Wrap a function call in a temporary directory that will clean up automatically if a local filesystem test."""
    if filesystem_cls == ezfs.LocalFilesystem:
        with tempfile.TemporaryDirectory(dir=".", prefix="pytest_") as tmpdir:
            filesystem_kwargs["directory"] = tmpdir
            return func(filesystem_cls, *args, filesystem_kwargs=filesystem_kwargs, **kwargs)
    else:
        return func(filesystem_cls, *args, filesystem_kwargs=filesystem_kwargs, **kwargs)


@pytest.mark.parametrize_test_case("test_case", TEST_CASES["file properties"])
def test_file_properties(test_case: dict, function_tester: Callable) -> None:
    """Test file object basic properties."""

    def _wrapper(
        filesystem_cls: type[ezfs.Filesystem],
        filesystem_kwargs: dict | None = None,
        file_kwargs: dict | None = None,
    ) -> dict:
        result = {}
        filesystem = filesystem_cls(**(filesystem_kwargs or {}))
        with filesystem.open(**(file_kwargs or {})) as file:
            result["str"] = str(file)
            result["repr"] = repr(file)
            if filesystem_cls == ezfs.LocalFilesystem:
                result["repr"] = result["repr"].replace(filesystem.directory, "PYTEST_TMP_DIR")
        return result

    function_tester(test_case, lambda *args, **kwargs: _tmpdir_wrapper(_wrapper, *args, **kwargs))


@pytest.mark.parametrize_test_case("test_case", TEST_CASES["filesystem"])
def test_filesystem(test_case: dict, function_tester: Callable) -> None:
    """Create a filesystem, and test common file read/write combinations."""

    def _wrapper(
        filesystem_cls: type[ezfs.Filesystem],
        filesystem_kwargs: dict | None = None,
        file_kwargs: dict | None = None,
        content: bytes | str = "",
    ) -> dict:
        result = {}
        filesystem = filesystem_cls(**(filesystem_kwargs or {}))
        name = file_kwargs.get("file")
        compression = file_kwargs.get("compression")
        transform = file_kwargs.get("transform")
        with filesystem.open(**(file_kwargs or {})) as file:
            if content:
                result["wrote"] = file.write(content)
            else:
                file.read()
        with filesystem.open(name, "rb", compression=ezfs.NO_COMPRESSION, transform=ezfs.NO_TRANSFORM) as file:
            result["raw"] = file.read()
        with filesystem.open(name, "rb", compression=compression, transform=transform) as file:
            result["read_bytes"] = file.read()
        with filesystem.open(name, "rt", compression=compression, transform=transform) as file:
            result["read_text"] = file.read()
        return result

    function_tester(test_case, lambda *args, **kwargs: _tmpdir_wrapper(_wrapper, *args, **kwargs))


@pytest.mark.parametrize_test_case("test_case", TEST_CASES["filesystem remove"])
def test_filesystem_remove(test_case: dict, function_tester: Callable) -> None:
    """Test filesystem remove operations."""

    def _wrapper(
        filesystem_cls: type[ezfs.Filesystem],
        filesystem_kwargs: dict | None = None,
        file_kwargs: dict | None = None,
        remove_kwargs: dict | None = None,
        skip_write: bool = False,
    ) -> bool:
        filesystem = filesystem_cls(**(filesystem_kwargs or {}))
        name = file_kwargs.get("file")
        if not skip_write:
            with filesystem.open(name, "wt") as file:
                file.write("test")
        remove_kwargs = remove_kwargs or {}
        name = remove_kwargs.pop("name") if "name" in remove_kwargs else name
        filesystem.remove(name, **(remove_kwargs or {}))
        try:
            with filesystem.open(name) as file:
                file.read()
        except FileNotFoundError:
            return True
        return False

    function_tester(test_case, lambda *args, **kwargs: _tmpdir_wrapper(_wrapper, *args, **kwargs))


@pytest.mark.parametrize_test_case("test_case", TEST_CASES["filesystem rename"])
def test_filesystem_rename(test_case: dict, function_tester: Callable) -> None:
    """Test filesystem rename operations."""

    def _wrapper(
        filesystem_cls: type[ezfs.Filesystem],
        src: str,
        dst: str,
        filesystem_kwargs: dict | None = None,
        rename_kwargs: dict | None = None,
        skip_write: bool = False,
    ) -> bool:
        filesystem = filesystem_cls(**(filesystem_kwargs or {}))
        if not skip_write:
            with filesystem.open(src, "wt") as file:
                file.write("test")
        filesystem.rename(src, dst, **(rename_kwargs or {}))
        try:
            with filesystem.open(src) as file:
                file.read()
        except FileNotFoundError:
            pass
        with filesystem.open(dst) as file:
            assert file.read() == "test"
        return True

    function_tester(test_case, lambda *args, **kwargs: _tmpdir_wrapper(_wrapper, *args, **kwargs))


def test_sqlite_row_factory() -> None:
    """Test sqlite connection override to change row factory."""
    filesystem = ezfs.SQLiteFilesystem()
    with filesystem.open("test", "w") as file_out:
        file_out.write("test content")
    assert ("test", b"test content") == filesystem.execute("SELECT * FROM files").fetchone()
    with pytest.raises(ValueError):
        dict(filesystem.execute("SELECT * FROM files").fetchone())

    class DictRows(ezfs.SQLiteFilesystem):
        def _connect(self) -> None:
            self._connection = sqlite3.connect(self.database)
            self._connection.row_factory = sqlite3.Row
            self._cursor = self._connection.cursor()

    filesystem = DictRows()
    with filesystem.open("test", "w") as file_out:
        file_out.write("test content")
    assert {"file": "test", "content": b"test content"} == dict(filesystem.execute("SELECT * FROM files").fetchone())


def test_transform_chain_with_compressor() -> None:
    """Test that a compressor can be mixed with plain transforms."""
    ezfs.Transform.chain(
        SWAP_TRANSFORM_1,
        SWAP_TRANSFORM_2,
        ezfs.Compressor(zstandard),
    )
