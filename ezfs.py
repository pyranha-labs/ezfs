"""Adapters for abstracting backend "file" storage from basic frontend read/write usage.

The backend storage for File objects may use any system, virtual or physical,
provided it can implement read, write, open, and close operations. Mode support varies
based on implementations. All types should support "r", "w", "b" and "t" at a minimum.

Compression is also supported natively by the Filesystem and File adapters. At open time,
a compression type can be specified, and it will be used to read/write file contents.
Compression support will vary by system. Compressors will automatically be detected
the first time a Filesystem is created, or `init_compressors()` can be called manually.

No additional functionality is guaranteed by File objects in order to maintain
simplicity and consistency across all forms of storage.

Includes the following basic adapters that can used directly, or as examples for more complex implementations:
- Local storage
- In-memory storage
- Remote storage (S3)
- Database storage (SQLite)
"""

from __future__ import annotations

import abc
import os
import re
import typing
from contextlib import contextmanager
from io import UnsupportedOperation
from types import ModuleType
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


__version__ = "1.0.1"
# Compressors may either be a module, or subclass of Compressor,
# with `compress()` and `decompress()` functions.
__COMPRESSORS__: dict[str, Compressor | ModuleType | None] = {
    None: None,
    "none": None,
}
__COMPRESSOR_SETUP_COMPLETE__ = False
NO_COMPRESSION = "none"


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
        # Use relative path within filesystem to avoid exposing full path in case it contains sensitive information.
        return self.file

    @abc.abstractmethod
    def close(self) -> None:
        """Close any open resources used by the file."""

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

    @contextmanager
    def open(self) -> File:
        """Open file for read and write operations, and perform automatic cleanup."""
        self._open_checks()
        try:
            yield self
        finally:
            self.close()

    @abc.abstractmethod
    def _read(self) -> bytes | str:
        """Read the contents of the file."""

    def read(self) -> bytes | str:
        """Read the contents of the file.

        Raises:
            UnsupportedOperation if the file is not writeable.
        """
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
        compression: str | Compressor | ModuleType | None = NO_COMPRESSION,
    ) -> None:
        """Initialize the base attributes of the filesystem for read and write operations.

        Args:
            compression: Default compression type to use when reading or writing file contents.
                Use basic string name to load from default compressor cache.
        """
        self.compression = compression
        if not __COMPRESSOR_SETUP_COMPLETE__:
            init_compressors()

    @abc.abstractmethod
    def _get_file(self, file: str, mode: str, encoding: str, compression: str) -> File:
        """Create file metadata object for read and write operations."""

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
        with self._get_file(file, mode, encoding, compression or self.compression).open() as open_file:
            yield open_file


class LocalFile(File):
    """File-like object on a local filesystem."""

    __slots__ = (
        "_file",
        "safe_path",
    )

    def __init__(  # pylint: disable=too-many-arguments
        self,
        filesystem: LocalFilesystem,
        file: str,
        mode: str = "rt",
        encoding: str = "utf-8",
        compression: str = NO_COMPRESSION,
        safe_path: bool = True,
    ) -> None:
        """Initialize the base attributes of the local file for read and write operations.

        Args:
            filesystem: Original filesystem used to create the File.
            file: Location of the file on the local filesystem.
            mode: Options used to open the file.
            encoding: Name of the encoding used to decode or encode the file when in text mode.
            compression: Type of compression used when reading or writing the file contents.
            safe_path: Whether safety checks are performed to prevent path escape attempts from filesystem directory.
                If paths are guaranteed to be safe, disable to maximize performance.
        """
        # Left strip to ensure that absolute paths are treated as relative, so that the filesystem directory is root.
        super().__init__(filesystem, file.lstrip(os.path.sep), mode=mode, encoding=encoding, compression=compression)
        self.filesystem: LocalFilesystem = filesystem  # Set again with typehint to avoid lint warnings.
        self._file = None
        self.safe_path = safe_path

    @override
    def __repr__(self) -> str:
        return os.path.abspath(os.path.join(self.filesystem.directory, self.file.lstrip(os.path.sep)))

    @override
    def close(self) -> None:
        self._file.close()
        self._file = None

    @override
    @contextmanager
    def open(self) -> File:
        # Do not call super full open checks, they will be performed by the native file open operation with local files.
        mode = self.mode
        if "t" not in mode and "b" not in mode:
            mode = f"{mode}t"

        path = os.path.join(self.filesystem.directory, self.file)
        if self.safe_path:
            path = os.path.abspath(path)

        # Validate final path to file, and treat as not found if invalid.
        if not path.startswith(self.filesystem.directory):
            raise FileNotFoundError(1, f"No such file: {self}")
        encoding = self.encoding
        if self.compression:
            # Force the mode to binary to allow utilizing native open operation to read/write compressed data.
            mode = mode.replace("t", "b")
            encoding = None
        self._file = open(path, mode, encoding=encoding)
        try:
            yield self
        finally:
            self.close()

    @override
    def _read_checks(self) -> None:
        # No checks are needed for local files, they will raise directly from native code.
        pass

    @override
    def _read(self) -> bytes | str:
        return self._file.read()

    @override
    def _write(self, data: bytes | str) -> int:
        return self._file.write(data)


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
        super().__init__(compression=compression)
        self.directory = os.path.abspath(directory)
        self.safe_paths = safe_paths

    @override
    def _get_file(self, file: str, mode: str, encoding: str, compression: str) -> File:
        return LocalFile(self, file, mode=mode, encoding=encoding, compression=compression, safe_path=self.safe_paths)


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
    def close(self) -> None:
        # No action required for in memory files.
        pass

    @override
    def _read(self) -> bytes | str:
        return self.filesystem.tree.get(self.file)

    @override
    def _read_checks(self) -> None:
        if "r" in self.mode and self.file not in self.filesystem.tree:
            raise FileNotFoundError(2, f"No such file: '{self.file}'")
        self._read_checks()

    @override
    def _write(self, data: bytes | str) -> int:
        self.filesystem.tree[self.file] = data
        return len(data)


class MemFilesystem(Filesystem, metaclass=abc.ABCMeta):
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
        super().__init__(compression=compression)
        self.tree = tree or {}

    @override
    def _get_file(self, file: str, mode: str, encoding: str, compression: str) -> File:
        return MemFile(self, file, mode=mode, encoding=encoding, compression=compression)


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
    def close(self) -> None:
        # No action required for S3 files.
        pass

    @override
    def _read(self) -> bytes | str:
        try:
            self._read_response = self.filesystem.client.get_object(Bucket=self.filesystem.bucket_name, Key=self.file)
            content = self._read_response["Body"].read()
        except self.filesystem.ClientError as client_error:
            # For consistency across File types, change missing objects errors to standard FileNotFoundErrors.
            if client_error.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(1, f"No such file: {self}") from client_error
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
        super().__init__(compression=compression)
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
    def _get_file(self, file: str, mode: str, encoding: str, compression: str) -> File:
        return S3BotoFile(self, file, mode=mode, encoding=encoding, compression=compression)


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
    def close(self) -> None:
        # No action required for database files.
        pass

    @override
    def _read(self) -> bytes | str:
        res = self.filesystem.execute(self.filesystem.read_query, (self.file,)).fetchone()
        if res is None:
            raise FileNotFoundError(2, f"No such file: '{self.file}'")
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
        super().__init__(compression=compression)
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
    def _get_file(self, file: str, mode: str, encoding: str, compression: str) -> SQLiteFile:
        return SQLiteFile(self, file, mode=mode, encoding=encoding, compression=compression)


def init_compressors() -> list[str]:
    """Search the system for available compression algorithms.

    Returns:
        List of the available compression types for reading and writing files.
    """
    global __COMPRESSOR_SETUP_COMPLETE__  # pylint: disable=global-statement
    __COMPRESSOR_SETUP_COMPLETE__ = True

    # pylint: disable=import-outside-toplevel
    # Builtin compression modules.
    try:
        import bz2

        __COMPRESSORS__["bz2"] = bz2
    except ModuleNotFoundError:
        pass
    try:
        import gzip

        __COMPRESSORS__["gzip"] = gzip
    except ModuleNotFoundError:
        pass
    try:
        import lzma

        __COMPRESSORS__["lzma"] = lzma
    except ModuleNotFoundError:
        pass

    # Third-party compression modules.
    try:
        import blosc

        __COMPRESSORS__["blosc"] = blosc
    except ModuleNotFoundError:
        pass
    try:
        import brotli

        __COMPRESSORS__["brotli"] = brotli
    except ModuleNotFoundError:
        pass
    try:
        import lz4.frame

        __COMPRESSORS__["lz4"] = lz4.frame
    except ModuleNotFoundError:
        pass
    try:
        import snappy

        __COMPRESSORS__["snappy"] = snappy
    except ModuleNotFoundError:
        pass
    try:
        import zstandard

        __COMPRESSORS__["zstd"] = zstandard
    except ModuleNotFoundError:
        pass

    return sorted(set(str(key).lower() for key in __COMPRESSORS__))
