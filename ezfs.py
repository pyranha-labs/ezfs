"""Adapters for abstracting backend "file" storage from basic frontend read/write usage.

File objects may use any backend storage, virtual or physical, provided it can implement read,
write, open, and close operations. Mode support varies based on implementation.

Filesystem objects provide common convenience methods for managing files based on `os` and `os.path`,
but support varies based on implementation.

Compression can be specified at filesystem creation time, and it will be used to read/write all files,
or at file open time, for a single file. Available compression types are automatically detected on filesystem creation.

Guaranteed functionality:
- File Modes: "r", "w", "b" and "t"
- File Methods: `open()`, `read()`, and `write()`
- Filesystem Methods: `open()`, `exists()`, `isfile()`, `remove()`, and `rename()`

No additional functionality is guaranteed by File or Filesystem objects in order to maintain simplicity and consistency.

Includes basic adapters for the following, which can used directly or as examples for more complex implementations:
- Local storage
- In-memory storage
- Remote storage (S3)
- Database storage (SQLite)
"""

from __future__ import annotations

import abc
import errno
import importlib
import os
import re
import typing
from contextlib import contextmanager
from io import UnsupportedOperation
from os import PathLike
from types import ModuleType
from types import TracebackType
from typing import Callable
from typing import Iterable

try:
    from typing import override  # pylint: disable=ungrouped-imports
except ImportError:
    try:
        from typing_extensions import override
    except ModuleNotFoundError:

        def override(func: Callable) -> Callable:
            """Passthrough decorator for environments where typing is not enabled."""
            return func


if typing.TYPE_CHECKING:
    try:
        import sqlite3
    except ModuleNotFoundError:

        class sqlite3:  # pylint: disable=invalid-name
            """Placeholder module for typing."""

            Cursor = None


__version__ = "1.0.2"
# Compressors may either be a module, or subclass of Compressor,
# with `compress()` and `decompress()` functions.
__COMPRESSORS__: dict[str, Compressor | ModuleType | None] = {}
NO_COMPRESSION = "none"
Path = str | bytes | PathLike[str] | PathLike[bytes]


class Compressor(metaclass=abc.ABCMeta):
    """Custom compressor to control compress/decompress logic."""

    @abc.abstractmethod
    def compress(self, data: bytes) -> bytes:
        """Shrink the data using a compression algorithm.

        Args:
            data: The original bytes to compress.

        Returns:
            The compressed bytes.
        """
        # Example: gzip.compress(data, compresslevel=1)

    @abc.abstractmethod
    def decompress(self, data: bytes) -> bytes:
        """Expand the data using a decompression algorithm.

        Args:
            data: The original bytes to decompress.

        Returns:
            The decompressed bytes.
        """
        # Example: gzip.decompress(data)


class File(metaclass=abc.ABCMeta):
    """Representation of a file-like object used for storage and retrival of data."""

    __slots__ = (
        "filesystem",
        "file",
        "mode",
        "encoding",
        "compression",
    )
    valid_modes = ("r", "w", "b", "t", "+")

    def __init__(
        self,
        filesystem: Filesystem,
        file: str,
        mode: str = "rt",
        encoding: str = "utf-8",
        compression: str | Compressor | ModuleType | None = NO_COMPRESSION,
    ) -> None:
        """Initialize the base attributes of the file for read and write operations.

        Args:
            filesystem: Original filesystem used to create the File.
            file: Location of the file in the filesystem.
            mode: Options used to open the file. See `File.valid_modes` and `builtins.open()` for details.
            encoding: Name of the encoding used to decode or encode the file when in text mode.
            compression: Compressor type to use when reading or writing the file contents.
                Use basic string name to load from default compressor cache.
        """
        self.filesystem = filesystem
        self.file = file
        self.mode = mode
        self.encoding = encoding
        self.compression = __COMPRESSORS__[compression] if isinstance(compression, str) else compression

    def __repr__(self) -> str:
        """Internal string representation of the file."""
        return f"{self.__class__.__name__.lower()}:{self.file}"

    def __str__(self) -> str:
        """External string representation of the file."""
        return self.file

    def __enter__(self) -> File:
        """Open a file for read and write operations.

        Return:
            This File in an open state as a context manager to handle read and write operations.
        """
        self._open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException],
        exc_value: BaseException,
        traceback: TracebackType,  # Preserve the original python name. pylint: disable=redefined-outer-name
    ) -> None:
        """Cleanup anc close the File when read and write operations are complete."""
        self._close()

    def _close(self) -> None:
        """Close any open resources used by the file."""
        # No actions required by default. Subclasses must inherit and override if they need to close resources.

    def _open(self) -> None:
        """Open file for read and write operations."""
        self._open_checks()

    def _open_checks(self) -> None:
        """Perform pre-checks before opening a file and raise exceptions matching local files."""
        mode = self.mode
        for char in mode:
            if char not in self.valid_modes:
                raise ValueError(f"Invalid mode: '{mode}'")
        if "t" not in mode and "b" not in mode:
            mode = f"{mode}t"
            self.mode = mode
        if "r" in mode and "w" in mode:
            raise ValueError("must have exactly one of read/write mode")
        if "t" in mode and "b" in mode:
            raise ValueError("can't have text and binary mode at once")

    @abc.abstractmethod
    def _read(self) -> bytes | str:
        """Read the contents of the file."""

    def read(self) -> bytes | str:
        """Read the contents of the file.

        Raises:
            UnsupportedOperation if the file is not writeable.
        """
        self._read_checks()
        data = self._read()
        if self.compression:
            data = self.compression.decompress(data)
        if isinstance(data, bytes) and "t" in self.mode:
            data = data.decode(self.encoding)
        return data

    def _read_checks(self) -> None:
        """Perform pre-checks before reading a file and raise exceptions matching local files."""
        if "r" not in self.mode:
            raise UnsupportedOperation("not readable")

    @abc.abstractmethod
    def _write(self, data: bytes | str) -> int:
        """Write the contents to the file."""

    def write(self, content: bytes | str) -> int:
        """Write the contents to the file.

        Args:
            content: The contents to write out to the file.

        Returns:
            Number of bytes written.

        Raises:
            UnsupportedOperation if the file is not writeable.
            TypeError if the contents are invalid.
        """
        self._write_checks(content)
        if self.compression:
            if isinstance(content, str):
                content = content.encode(self.encoding)
            content = self.compression.compress(content)
        return self._write(content)

    def _write_checks(self, content: bytes | str) -> None:
        """Perform pre-checks before writing a file and raise exceptions matching local filesystem behavior."""
        if "w" not in self.mode:
            raise UnsupportedOperation("not writeable")
        if not isinstance(content, (bytes, str)):
            raise TypeError("write() argument must be bytes or str")
        if isinstance(content, bytes) and "b" not in self.mode:
            raise TypeError("write() argument must be str, not bytes")
        if isinstance(content, str) and "t" not in self.mode:
            raise TypeError("write() argument must be bytes, not str")
        if not isinstance(content, (bytes, str)):
            raise TypeError("write() argument must be bytes, not str")


class Filesystem(metaclass=abc.ABCMeta):
    """Collection of file-like objects used for storage and retrival of data."""

    def __init__(
        self,
        file_type: type[File],
        compression: str | Compressor | ModuleType | None = NO_COMPRESSION,
    ) -> None:
        """Initialize the base attributes of the filesystem for read and write operations.

        Args:
            file_type: File adapter type to use when opening files for read and write operations.
            compression: Default compression type to use when reading or writing file contents.
                Use basic string name to load from default compressor cache.
        """
        self.ftype = file_type
        self.compression = compression
        if not __COMPRESSORS__:
            init_compressors()

    @contextmanager
    def open(
        self,
        file: str,
        mode: str = "rt",
        encoding: str = "utf-8",
        compression: str | None = None,
    ) -> File:
        """Open file for read and write operations, and perform automatic cleanup.

        Args:
            file: Location of the file in the filesystem.
            mode: Options used to open the file. See "open()" for details.
            encoding: Name of the encoding used to decode or encode the file when in text mode.
            compression: Type of compression used when reading or writing the file contents.
                Use `None` to default to the Filesystem compression type.
                Default Filesystem compression is no compression.
        """
        with self.ftype(self, file, mode=mode, encoding=encoding, compression=compression or self.compression) as _file:
            yield _file

    def exists(self, path: str | bytes | PathLike[str] | PathLike[bytes]) -> bool:
        """Check whether path refers to an existing location in the filesystem.

        Behavior mirrors `os.path.exists()` as closely as possible if supported by the filesystem.

        Args:
            path: Location of the file in the filesystem.

        Returns:
            True if the path is found, False otherwise.
        """
        return self.isfile(path)

    @abc.abstractmethod
    def isfile(self, path: str | bytes | PathLike[str] | PathLike[bytes]) -> bool:
        """Check if path is a regular file.

        Behavior mirrors `os.path.isfile()` as closely as possible if supported by the filesystem.

        Args:
            path: Location of the file in the filesystem.

        Returns:
            True if the path is a regular file, False otherwise.
        """

    @abc.abstractmethod
    def _remove(self, path: Path, *, dir_fd: int | None = None) -> None:
        """Remove (delete) the file path."""

    def remove(self, path: Path, *, dir_fd: int | None = None) -> None:
        """Remove (delete) the file path.

        Behavior mirrors `os.remove()` as closely as possible if supported by the filesystem.

        Args:
            path: Location of the file in the filesystem.
            dir_fd: File descriptor open to a directory, which will change path to be relative to that directory.

        Raises:
            FileNotFoundError if path is not found.
            OSError if path is a directory.
            NotImplementedError if dir_fd is used and not available for the platform or Filesystem.
        """
        if dir_fd is not None:
            # Non-local filesystems do not support dir_fd. Default to not allowed.
            raise NotImplementedError(f"dir_fd is not supported by {self.__class__.__name__}")
        if not self.exists(path):
            raise FileNotFoundError(errno.ENOENT, f"No such file or directory: {path}")
        if not self.isfile(path):
            raise OSError(1, f"Operation not permitted: {path}")
        self._remove(path, dir_fd=dir_fd)

    @abc.abstractmethod
    def _rename(self, src: Path, dst: Path, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None) -> None:
        """Rename (move) the file from source to destination."""

    def rename(self, src: Path, dst: Path, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None) -> None:
        """Rename (move) the file from source to destination.

        Behavior mirrors `os.rename()` as closely as possible if supported by the filesystem.

        Args:
            src: Current location of the file in the filesystem.
            dst: Target location of the file in the filesystem.
            src_dir_fd: File descriptor open to a directory, which will change src path to be relative to that directory.
            dst_dir_fd: File descriptor open to a directory, which will change dst path to be relative to that directory.

        Raises:
            FileNotFoundError if path is not found.
            OSError if path is a directory.
            NotImplementedError if a dir_fd arg is used and not available for the platform or Filesystem.
            FileExistsError if the destination already exists.
        """
        if src_dir_fd is not None or dst_dir_fd is not None:
            # Non-local filesystems do not support dir_fd(s). Default to not allowed.
            raise NotImplementedError(f"src_dir_fd and dst_dir_fd are not supported by {self.__class__.__name__}")
        if not self.exists(src):
            raise FileNotFoundError(errno.ENOENT, f"No such file or directory: {src}")
        if not self.isfile(src):
            raise OSError(1, f"Operation not permitted: {src}")
        if self.exists(dst):
            raise FileExistsError(errno.EEXIST, f"File exists: {dst}")
        self._rename(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)


class LocalFile(File):
    """File-like object on a local filesystem."""

    __slots__ = ("_file",)

    def __init__(  # pylint: disable=too-many-arguments
        self,
        filesystem: LocalFilesystem,
        file: str,
        mode: str = "rt",
        encoding: str = "utf-8",
        compression: str = NO_COMPRESSION,
    ) -> None:
        """Initialize the base attributes of the local file for read and write operations.

        Args:
            filesystem: Original filesystem used to create the File.
            file: Location of the file on the local filesystem.
            mode: Options used to open the file.
            encoding: Name of the encoding used to decode or encode the file when in text mode.
            compression: Type of compression used when reading or writing the file contents.
        """
        # Left strip to ensure that absolute paths are treated as relative, so that the filesystem directory is root.
        super().__init__(filesystem, file, mode=mode, encoding=encoding, compression=compression)
        self.filesystem: LocalFilesystem = filesystem  # Set again with typehint to avoid lint warnings.
        self._file = None

    @override
    def __str__(self) -> str:
        # Use relative path within filesystem to avoid exposing full path in case it contains sensitive information.
        return self.file.replace(self.filesystem.directory, "")

    @override
    def _close(self) -> None:
        self._file.close()
        self._file = None

    @override
    def _open(self) -> None:
        # Do not call super full open checks, they will be performed by the native file open operation with local files.
        mode = self.mode
        if "t" not in mode and "b" not in mode:
            mode = f"{mode}t"

        encoding = self.encoding
        if self.compression:
            # Force the mode to binary to allow utilizing native open operation to read/write compressed data.
            mode = mode.replace("t", "b")
            encoding = None
        self._file = open(self.file, mode, encoding=encoding)  # pylint: disable=consider-using-with

    @override
    def _read(self) -> bytes | str:
        return self._file.read()

    @override
    def _read_checks(self) -> None:
        # No checks are needed for local files, they will raise directly from native code.
        pass

    @override
    def _write(self, data: bytes | str) -> int:
        return self._file.write(data)

    @override
    def _write_checks(self, content: bytes | str) -> None:
        # No checks are needed for local files, they will raise directly from native code.
        pass


class LocalFilesystem(Filesystem):
    """Collection of file-like objects available on a local filesystem."""

    def __init__(
        self,
        directory: str,
        compression: str = NO_COMPRESSION,
        safe_paths: bool = True,
    ) -> None:
        """Initialize the base attributes of the local filesystem for read and write operations.

        Args:
            directory: Root directory for all files available in the filesystem.
            compression: Default compression type to use when reading or writing file contents.
                Use basic string name to load from default compressor cache.
            safe_paths: Whether safety checks are performed to prevent path escape attempts from filesystem directory.
                If paths are guaranteed to be safe, disable to maximize performance.
        """
        super().__init__(LocalFile, compression=compression)
        self.ftype = LocalFile  # Set again to avoid lint errors.
        self.directory = os.path.abspath(directory)
        self.safe_paths = safe_paths

    @override
    @contextmanager
    def open(
        self,
        file: str,
        mode: str = "rt",
        encoding: str = "utf-8",
        compression: str | None = None,
    ) -> File:
        path = os.path.abspath(os.path.join(self.directory, file.lstrip(os.path.sep)))
        if self.safe_paths:
            # Validate final path to file, and treat as not found if attempting to escape the root.
            if not path.startswith(self.directory):
                raise FileNotFoundError(errno.ENOENT, f"No such file or directory: {file}")

        with self.ftype(self, path, mode=mode, encoding=encoding, compression=compression or self.compression) as _file:
            yield _file

    @override
    def exists(self, path: str | bytes | PathLike[str] | PathLike[bytes]) -> bool:
        return os.path.exists(path)

    @override
    def isfile(self, path: str | bytes | PathLike[str] | PathLike[bytes]) -> bool:
        return os.path.isfile(path)

    @override
    def _remove(self, path: str | bytes | PathLike[str] | PathLike[bytes], *, dir_fd: int | None = None) -> None:
        pass

    @override
    def remove(self, path: str | bytes | PathLike[str] | PathLike[bytes], *, dir_fd: int | None = None) -> None:
        # Bypass base remove() checks to allow support for dir_fd, and 1-to-1 match with native behavior.
        os.remove(path, dir_fd=dir_fd)

    @override
    def _rename(self, src: Path, dst: Path, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None) -> None:
        pass

    @override
    def rename(self, src: Path, dst: Path, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None) -> None:
        # Bypass base rename() checks to allow support for dir_fd arguments, and 1-to-1 match with native behavior.
        os.rename(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)


class MemFile(File):
    """File-like object stored in memory."""

    def __init__(
        self,
        filesystem: MemFilesystem,
        file: str,
        mode: str = "rt",
        encoding: str = "utf-8",
        compression: str = NO_COMPRESSION,
    ) -> None:
        """Initialize the base attributes of the in-memory file for read and write operations.

        Args:
            filesystem: Original filesystem used to create the File.
            file: Location of the file in the in-memory filesystem.
            mode: Options used to open the file.
            encoding: Name of the encoding used to decode or encode the file when in text mode.
            compression: Type of compression used when reading or writing the file contents.
        """
        super().__init__(filesystem, file, mode=mode, encoding=encoding, compression=compression)
        self.filesystem: MemFilesystem = filesystem  # Set again with typehint to avoid lint warnings.

    @override
    def _read(self) -> bytes | str:
        return self.filesystem.tree.get(self.file)

    @override
    def _read_checks(self) -> None:
        if "r" in self.mode and self.file not in self.filesystem.tree:
            raise FileNotFoundError(errno.ENOENT, f"No such file: '{self.file}'")
        super()._read_checks()

    @override
    def _write(self, data: bytes | str) -> int:
        self.filesystem.tree[self.file] = data
        return len(data)


class MemFilesystem(Filesystem):
    """Collection of file-like objects available in an in-memory filesystem."""

    def __init__(
        self,
        tree: dict[str, bytes | str] | None = None,
        compression: str = NO_COMPRESSION,
    ) -> None:
        """Initialize the base attributes of the in-memory filesystem for read and write operations.

        Args:
            tree: Initial virtual filesystem tree contents.
            compression: Default compression type to use when reading or writing file contents.
                Use basic string name to load from default compressor cache.
        """
        super().__init__(MemFile, compression=compression)
        self.tree = tree or {}

    @override
    def isfile(self, path: str | bytes | PathLike[str] | PathLike[bytes]) -> bool:
        return str(path) in self.tree

    @override
    def _remove(self, path: str | bytes | PathLike[str] | PathLike[bytes], *, dir_fd: int | None = None) -> None:
        self.tree.pop(str(path))

    @override
    def _rename(self, src: Path, dst: Path, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None) -> None:
        self.tree[str(dst)] = self.tree.pop(str(src))


class S3BotoFile(File):
    """File-like object in a remote S3 bucket."""

    __slots__ = (
        "_read_response",
        "_write_response",
    )

    def __init__(
        self,
        filesystem: S3BotoFilesystem,
        file: str,
        mode: str = "rt",
        encoding: str = "utf-8",
        compression: str = NO_COMPRESSION,
    ) -> None:
        """Initialize the base attributes of the file-like S3 object for read and write operations.

        Args:
            filesystem: Original filesystem used to create the File.
            file: Location of the object in the S3 bucket.
            mode: Options used to open the file.
            encoding: Name of the encoding used to decode or encode the file when in text mode.
            compression: Type of compression used when reading or writing the file contents.
        """
        super().__init__(filesystem, file, mode=mode, encoding=encoding, compression=compression)
        self.filesystem: S3BotoFilesystem = filesystem  # Set again with typehint to avoid lint warnings.
        self._read_response = None
        self._write_response = None

    @override
    def __repr__(self) -> str:
        return f"{self.filesystem.bucket_name}:{self.file}"

    @override
    def _read(self) -> bytes | str:
        try:
            self._read_response = self.filesystem.client.get_object(Bucket=self.filesystem.bucket_name, Key=self.file)
            content = self._read_response["Body"].read()
        except self.filesystem.ClientError as client_error:
            # For consistency across File types, change missing objects errors to standard FileNotFoundErrors.
            if client_error.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(errno.ENOENT, f"No such file: {self}") from client_error
            raise client_error
        return content

    @property
    def read_response(self) -> dict | None:
        """The raw S3 response after a read operation is performed."""
        return self._read_response

    @override
    def _write(self, data: bytes | str) -> int:
        self._write_response = self.filesystem.client.put_object(
            Body=data,
            Bucket=self.filesystem.bucket_name,
            Key=self.file,
        )
        return len(data)

    @property
    def write_response(self) -> dict | None:
        """The raw S3 response after a write operation is performed."""
        return self._write_response


class S3BotoFilesystem(Filesystem):
    """Collection of file-like objects available in a remote S3 bucket."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        bucket_name: str,
        access_key_id: str = None,
        secret_access_key: str = None,
        region_name: str = None,
        profile_name: str = None,
        compression: str = NO_COMPRESSION,
    ) -> None:
        """Initialize the base attributes of the S3 filesystem for read and write operations.

        Args:
            bucket_name: Name of the bucket with all objects available as files in the filesystem.
            access_key_id: Access key ID with permission to read/write to the bucket.
            secret_access_key: AWS secret access key with permission to read/write to the bucket.
            region_name: Default region when creating bucket connection.
            profile_name: Name of a custom profile to use, instead of default.
            compression: Default compression type to use when reading or writing file contents.
                Use basic string name to load from default compressor cache.
        """
        super().__init__(S3BotoFile, compression=compression)
        try:
            # pylint: disable=import-outside-toplevel,invalid-name
            import boto3
            from botocore.exceptions import ClientError

            self.ClientError = ClientError
        except ModuleNotFoundError as error:
            raise ModuleNotFoundError(f"boto3 and botocore are required to use {self.__class__.__name__}") from error

        self.bucket_name = bucket_name
        self._session = boto3.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region_name,
            profile_name=profile_name,
        )
        self.client = self._session.client("s3")

    @override
    def isfile(self, path: str | bytes | PathLike[str] | PathLike[bytes]) -> bool:
        isfile = True
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=str(path))
        except self.ClientError as client_error:
            if not client_error.response["Error"]["Code"] == "404":
                raise
            isfile = False
        return isfile

    @override
    def _remove(self, path: str | bytes | PathLike[str] | PathLike[bytes], *, dir_fd: int | None = None) -> None:
        self.client.delete_object(Bucket=self.bucket_name, Key=str(path))

    @override
    def _rename(self, src: Path, dst: Path, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None) -> None:
        cp_src = {"Bucket": self.bucket_name, "Key": src}
        self.client.copy_object(Bucket=self.bucket_name, Key=str(dst), CopySource=cp_src)
        self._remove(src)


class SQLiteFile(File):
    """File-like object in a SQLite database."""

    def __init__(
        self,
        filesystem: SQLiteFilesystem,
        file: str,
        mode: str = "rt",
        encoding: str = "utf-8",
        compression: str = NO_COMPRESSION,
    ) -> None:
        """Initialize the base attributes of the file-like database object for read and write operations.

        Args:
            filesystem: Original filesystem used to create the File.
            file: Location of the object in the database table.
            mode: Options used to open the file.
            encoding: Name of the encoding used to decode or encode the file when in text mode.
            compression: Type of compression used when reading or writing the file contents.
        """
        super().__init__(filesystem, file, mode=mode, encoding=encoding, compression=compression)
        self.filesystem: SQLiteFilesystem = filesystem  # Set again with typehint to avoid lint warnings.

    @override
    def __repr__(self) -> str:
        return f"sqlite3://{self.filesystem.database}?table_name={self.filesystem.table_name}&file={self.file}"

    @override
    def _read(self) -> bytes | str:
        res = self.filesystem.execute(self.filesystem.read_query, (self.file,)).fetchone()
        if res is None:
            raise FileNotFoundError(errno.ENOENT, f"No such file: '{self.file}'")
        content = res[0]
        return content

    @override
    def _write(self, data: bytes | str) -> int:
        self.filesystem.execute(self.filesystem.write_query, (self.file, data, data))
        self.filesystem.commit()
        return len(data)


class SQLiteFilesystem(Filesystem):
    """Collection of file-like objects available in a database using SQLite."""

    def __init__(
        self,
        database: str,
        table_name: str = "files",
        file_col: str = "file",
        content_col: str = "content",
        compression: str = NO_COMPRESSION,
    ) -> None:
        """Initialize the base attributes of the database filesystem for read and write operations.

        Args:
            database: The path to the database file to be opened. e.g., "example.db", ":memory:", etc.
            table_name: Name of the table with data available as files in the filesystem.
            file_col: Name of the column in the table that contains the path to the files.
            content_col: Name of the column in the table that contains the raw contents for the files.
            compression: Default compression type to use when reading or writing file contents.
                Use basic string name to load from default compressor cache.
        """
        for name, value in (
            ("table_name", table_name),
            ("file_col", file_col),
            ("content_col", content_col),
        ):
            if not re.match(r"^[A-za-z0-9_]+$", value):
                raise ValueError(f"{name} may only contain letters, numbers, and underscores.")
        super().__init__(SQLiteFile, compression=compression)
        try:
            # pylint: disable=import-outside-toplevel,redefined-outer-name,reimported
            import sqlite3

        except ModuleNotFoundError as error:
            raise ModuleNotFoundError(f"sqlite3 is required to use {self.__class__.__name__}") from error

        # Save the database and table name to allow string representations in files,
        # but cache the templates to prevent modifications from impacting later execution.
        self.database = database
        self.table_name = table_name  # Save the table name to allow
        self._connection = sqlite3.connect(database)
        self._cursor = self._connection.cursor()

        # Cache the query strings to reduce overhead.
        # "nosec" added due to validation before this point.
        self._create_query = f"CREATE TABLE {table_name}({file_col} TEXT(255) PRIMARY KEY, {content_col} BLOB)"  # nosec
        self.read_query = f"SELECT {content_col} FROM {table_name} WHERE {file_col} = (?) LIMIT 1"  # nosec
        self.write_query = (
            f"INSERT INTO {table_name}({file_col}, {content_col}) VALUES(?, ?) "  # nosec
            f"ON CONFLICT({file_col}) DO UPDATE SET {content_col}=?;"
        )
        self.exists_query = f"SELECT {file_col} FROM {table_name} WHERE {file_col}=(?) LIMIT 1;"  # nosec
        self.remove_query = f"DELETE FROM {table_name} WHERE {file_col}=(?);"  # nosec
        self.rename_query = f"UPDATE {table_name} SET {file_col}=(?) WHERE {file_col}=(?);"  # nosec

    def commit(self) -> None:
        """Commit any pending transactions to the database backend."""
        self._connection.commit()

    def create_table(self) -> None:
        """Create a basic table to use for storage."""
        self.execute(self._create_query)

    def execute(self, sql: str, params: Iterable | dict = ()) -> sqlite3.Cursor:
        """Execute a SQL statement against the backend storage table.

        Args:
            sql: A single SQL statement.
            params: Iterable of values to bind to placeholders in sql, or dict if named placeholders are used.

        Returns:
            The cursor used to execute the statement, allowing caller to fetch results.
        """
        return self._cursor.execute(sql, params)

    @override
    def isfile(self, path: str | bytes | PathLike[str] | PathLike[bytes]) -> bool:
        isfile = True
        res = self.execute(self.exists_query, (str(path),)).fetchone()
        if res is None:
            isfile = False
        return isfile

    @override
    def _remove(self, path: str | bytes | PathLike[str] | PathLike[bytes], *, dir_fd: int | None = None) -> None:
        self.execute(self.remove_query, (str(path),))
        self.commit()

    @override
    def _rename(self, src: Path, dst: Path, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None) -> None:
        self.execute(self.rename_query, (str(dst), str(src)))
        self.commit()


def init_compressors() -> list[str]:
    """Search the system for available compression algorithms.

    Returns:
        List of the available compression types for reading and writing files.
    """
    __COMPRESSORS__.update({None: None, NO_COMPRESSION: None})
    libs = (
        # Builtin compression modules.
        ("bz2",),
        ("gzip",),
        ("lzma",),
        # Third-party compression modules.
        ("blosc",),
        ("brotli",),
        ("lz4", "lz4.frame"),
        ("snappy",),
        ("zstd", "zstandard"),
    )
    for lib in libs:
        name, module_name = lib if len(lib) == 2 else (lib[0], lib[0])  # pylint: disable=unbalanced-tuple-unpacking
        try:
            __COMPRESSORS__[name] = importlib.import_module(module_name)
        except ImportError:
            pass
    return sorted(set(str(key).lower() for key in __COMPRESSORS__))
