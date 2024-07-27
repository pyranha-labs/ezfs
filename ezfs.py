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
from typing import Any
from typing import Callable
from typing import Generic
from typing import Iterable
from typing import TypeVar

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


__version__ = "1.1.1"
__COMPRESSORS__: dict[str, Transform | None] = {}
NO_TRANSFORM = "none"
NO_COMPRESSION = NO_TRANSFORM
Path = str | bytes | PathLike[str] | PathLike[bytes]


class Transform(metaclass=abc.ABCMeta):
    """Transformation to apply to raw bytes before writing to, or after reading from, raw storage.

    Common transformations include compression/decompression, encoding/decoding, and encrypting/decrypting.
    Transforms may perform any operations against raw bytes, as long as they output raw bytes, and are reversible.

    Transforms should be stateless, and allow reuse across multiple files after instantiation.
    """

    def __init__(self, apply: Callable[[bytes], bytes], remove: Callable[[bytes], bytes]) -> None:
        """Initialize the transformation with byte modification operations.

        Args:
            apply: The function to call to modify the data before writing.
            remove: The function to call to undo the operation when reading.
        """
        self._dependent = None
        self._apply = apply
        self._remove = remove

    def apply(self, data: bytes) -> bytes:
        """Run the transformation against raw data.

        Args:
            data: The original bytes to transform.

        Returns:
            The transformed bytes.
        """
        data = self._apply(data)
        if self._dependent:
            data = self._dependent.apply(data)
        return data

    @staticmethod
    def chain(*transforms: Transform) -> Transform:
        """Combine multiple transformations together to modify data on read and write.

        Args:
            transforms: All transformations to apply to the data.
                Transformations applied in ascending order on write, and descending order on read.

        Returns:
            The primary transformation to use to start applications and removals.
        """
        transforms = [transform._copy() for transform in transforms]  # pylint: disable=protected-access
        for index, transform in enumerate(transforms):
            if transform != transforms[-1]:
                transform._dependent = transforms[index + 1]  # pylint: disable=protected-access
        return transforms[0]

    def _copy(self) -> Transform:
        """Create a copy of this transformation to allow use in chained operations without modifying original."""
        return type(self)(self._apply, self._remove)

    def remove(self, data: bytes) -> bytes:
        """Reverse the transformation on previously transformed data.

        Args:
            data: The bytes with the transformation applied.

        Returns:
            The original bytes without the transformation removed.
        """
        if self._dependent:
            data = self._dependent.remove(data)
        data = self._remove(data)
        return data


class Compressor(Transform):
    """Transform data using a compression/decompression module."""

    def __init__(
        self,
        compressor: ModuleType,
        compress_kwargs: Any | None = None,
        decompress_kwargs: Any | None = None,
    ) -> None:
        """Initialize the compressor with a specific compression module.

        Args:
            compressor: The module, or object, with `compress()` and `decompress()` functions.
            compress_kwargs: Keyword arguments to use during compression. Support varies by compressor.
            decompress_kwargs: Keyword arguments to use during decompression. Support varies by compressor.
        """
        super().__init__(self._compress, self._decompress)
        self.compressor = compressor
        self.compression_kwargs = dict(compress_kwargs or {})
        self.decompression_kwargs = dict(decompress_kwargs or {})

    @override
    def _copy(self) -> Transform:
        return type(self)(self.compressor, self.compression_kwargs.copy(), self.decompression_kwargs.copy())

    def _compress(self, data: bytes) -> bytes:
        """Compress the data using the provided module."""
        return self.compressor.compress(data, **self.compression_kwargs)

    def _decompress(self, data: bytes) -> bytes:
        """Decompress the data using the provided module."""
        return self.compressor.decompress(data, **self.decompression_kwargs)


class Filesystem(metaclass=abc.ABCMeta):
    """Collection of file-like objects used for storage and retrival of data."""

    def __init__(
        self,
        file_type: type[File],
        compression: str | Transform | None = NO_COMPRESSION,
        transform: Transform | None = None,
    ) -> None:
        """Initialize the base attributes of the filesystem for read and write operations.

        Args:
            file_type: File adapter type to use when opening files for read and write operations.
            compression: Default compression type to use when reading or writing file contents.
                Use basic string name to load from default compressor cache.
            transform: Default transformation used when reading or writing the file contents.
                Transformations are applied before compression when writing, and after decompression when reading.
        """
        if not __COMPRESSORS__:
            init_compressors()
        self.ftype = file_type
        self.compression = compression
        self.transform = transform

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

    @contextmanager
    def open(
        self,
        file: str,
        mode: str = "rt",
        encoding: str = "utf-8",
        compression: str | Transform | None = None,
        transform: Transform | None = None,
    ) -> File:
        """Open file for read and write operations, and perform automatic cleanup.

        Args:
            file: Location of the file in the filesystem.
            mode: Options used to open the file. See "open()" for details.
            encoding: Name of the encoding used to decode or encode the file when in text mode.
            compression: Type of compression used when reading or writing the file contents.
                Use `None` to default to the Filesystem compression type.
            transform: Type of transformation used when reading or writing the file contents.
                Use `None` to default to the Filesystem transformation type.

        Yields:
            An open file usable as a context manager for read and write operations.
        """
        with self.ftype(
            self,
            file,
            mode=mode,
            encoding=encoding,
            compression=compression or self.compression,
            transform=transform or self.transform,
        ) as _file:
            yield _file

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
            raise OSError(errno.EPERM, f"Operation not permitted: {path}")
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
            raise OSError(errno.EPERM, f"Operation not permitted: {src}")
        if self.exists(dst):
            raise FileExistsError(errno.EEXIST, f"File exists: {dst}")
        self._rename(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)


FilesystemType = TypeVar("FilesystemType", bound=Filesystem)  # pylint: disable=invalid-name


class File(Generic[FilesystemType], metaclass=abc.ABCMeta):
    """Representation of a file-like object used for storage and retrival of data."""

    __slots__ = (
        "filesystem",
        "file",
        "mode",
        "encoding",
        "compression",
        "transform",
    )
    valid_modes = ("r", "w", "b", "t", "+")
    skip_write_encode = False

    def __init__(  # pylint: disable=too-many-arguments
        self,
        filesystem: FilesystemType,
        file: str,
        mode: str = "rt",
        encoding: str = "utf-8",
        compression: str | Transform | None = NO_COMPRESSION,
        transform: Transform | None = None,
    ) -> None:
        """Initialize the base attributes of the file for read and write operations.

        Args:
            filesystem: Original filesystem used to create the File.
            file: Location of the file in the filesystem.
            mode: Options used to open the file. See `File.valid_modes` and `builtins.open()` for details.
            encoding: Name of the encoding used to decode or encode the file when in text mode.
            compression: Compressor type to use when reading or writing the file contents.
                Use basic string name to load from default compressor cache.
            transform: Data transformation used when reading or writing the file contents.
                Transformations are applied before compression when writing, and after decompression when reading.
        """
        self.filesystem = filesystem
        self.file = file
        self.mode = mode
        self.encoding = encoding
        self.compression = __COMPRESSORS__[compression] if isinstance(compression, str) else compression
        self.transform = transform if transform != NO_TRANSFORM else None

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
    def _read(self) -> bytes:
        """Read the raw contents of the file."""

    def read(self) -> bytes | str:
        """Read the contents of the file.

        Raises:
            UnsupportedOperation if the file is not readable.
        """
        self._read_checks()
        data = self._read()
        if self.compression:
            data = self.compression.remove(data)
        if self.transform:
            data = self.transform.remove(data)
        if isinstance(data, bytes) and "t" in self.mode:
            data = data.decode(self.encoding)
        return data

    def _read_checks(self) -> None:
        """Perform pre-checks before reading a file and raise exceptions matching local files."""
        if "r" not in self.mode:
            raise UnsupportedOperation("not readable")

    @abc.abstractmethod
    def _write(self, data: bytes) -> int:
        """Write the raw contents to the file."""

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
        if (self.compression or self.transform) and isinstance(content, str):
            content = content.encode(self.encoding)
        if self.transform:
            content = self.transform.apply(content)
        if self.compression:
            content = self.compression.apply(content)
        if not self.skip_write_encode and isinstance(content, str):
            content = content.encode(self.encoding)
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


class LocalFilesystem(Filesystem):
    """Collection of file-like objects available on a local filesystem."""

    def __init__(
        self,
        directory: str,
        compression: str | Transform | None = NO_COMPRESSION,
        transform: Transform | None = None,
        safe_paths: bool = True,
    ) -> None:
        """Initialize the base attributes of the local filesystem for read and write operations.

        Args:
            directory: Root directory for all files available in the filesystem.
            compression: Default compression type to use when reading or writing file contents.
            transform: Default transformation used when reading or writing file contents.
            safe_paths: Whether safety checks are performed to prevent path escape attempts from filesystem directory.
                If paths are guaranteed to be safe, disable to maximize performance.
        """
        super().__init__(LocalFile, compression=compression, transform=transform)
        self.directory = os.path.abspath(directory)
        self.safe_paths = safe_paths

    @override
    @contextmanager
    def open(
        self,
        file: str,
        mode: str = "rt",
        encoding: str = "utf-8",
        compression: str | Transform | None = None,
        transform: Transform | None = None,
    ) -> LocalFile:
        path = os.path.abspath(os.path.join(self.directory, file.lstrip(os.path.sep)))
        if self.safe_paths:
            # Validate final path to file, and treat as not found if attempting to escape the root.
            if not path.startswith(self.directory):
                raise FileNotFoundError(errno.ENOENT, f"No such file or directory: {file}")

        with self.ftype(
            self,
            path,
            mode=mode,
            encoding=encoding,
            compression=compression or self.compression,
            transform=transform or self.transform,
        ) as _file:
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
        if self.safe_paths:
            # Validate final path to file, and treat as not found if attempting to escape the root.
            path = self._validate(path)
        # Bypass base remove() checks to allow support for dir_fd, and 1-to-1 match with native behavior.
        os.remove(path, dir_fd=dir_fd)

    @override
    def _rename(self, src: Path, dst: Path, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None) -> None:
        pass

    @override
    def rename(self, src: Path, dst: Path, *, src_dir_fd: int | None = None, dst_dir_fd: int | None = None) -> None:
        if self.safe_paths:
            # Validate final path to files, and treat as not found if attempting to escape the root.
            src = self._validate(src)
            dst = self._validate(dst)
        # Bypass base rename() checks to allow support for dir_fd arguments, and 1-to-1 match with native behavior.
        os.rename(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

    def _validate(self, path: str) -> str:
        """Ensure a path is within the directory boundary for this filesystem."""
        final_path = os.path.abspath(os.path.join(self.directory, path.lstrip(os.path.sep)))
        if not final_path.startswith(self.directory):
            raise FileNotFoundError(errno.ENOENT, f"No such file or directory: {path}")
        return final_path


class LocalFile(File[LocalFilesystem]):
    """File-like object on a local filesystem."""

    __slots__ = ("_file",)
    skip_write_encode = True

    @override
    def __str__(self) -> str:
        # Use relative path within filesystem to avoid exposing full path in case it contains sensitive information.
        return self.file.replace(self.filesystem.directory, "")

    @override
    def _close(self) -> None:
        self._file.close()

    @override
    def _open(self) -> None:
        # Do not call super full open checks, they will be performed by the native file open operation with local files.
        mode = self.mode
        if "t" not in mode and "b" not in mode:
            mode = f"{mode}t"

        encoding = self.encoding
        if self.compression or self.transform:
            # Force the mode to binary to allow utilizing native open operation to read/write compressed data.
            mode = mode.replace("t", "b")
            encoding = None
        if "b" in mode:
            encoding = None
        self._file = open(self.file, mode, encoding=encoding)  # pylint: disable=attribute-defined-outside-init,consider-using-with

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


class MemFilesystem(Filesystem):
    """Collection of file-like objects available in an in-memory filesystem."""

    def __init__(
        self,
        tree: dict[str, bytes | str] | None = None,
        compression: str | Transform | None = NO_COMPRESSION,
        transform: Transform | None = None,
    ) -> None:
        """Initialize the base attributes of the in-memory filesystem for read and write operations.

        Args:
            tree: Initial virtual filesystem tree contents.
            compression: Default compression type to use when reading or writing file contents.
            transform: Default transformation used when reading or writing file contents.
        """
        super().__init__(MemFile, compression=compression, transform=transform)
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


class MemFile(File[MemFilesystem]):
    """File-like object stored in memory."""

    __slots__ = ()

    @override
    def _read(self) -> bytes:
        return self.filesystem.tree.get(self.file)

    @override
    def _read_checks(self) -> None:
        if "r" in self.mode and self.file not in self.filesystem.tree:
            raise FileNotFoundError(errno.ENOENT, f"No such file: '{self.file}'")
        super()._read_checks()

    @override
    def _write(self, data: bytes) -> int:
        self.filesystem.tree[self.file] = data
        return len(data)


class S3BotoFilesystem(Filesystem):
    """Collection of file-like objects available in a remote S3 bucket."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        bucket_name: str,
        access_key_id: str = None,
        secret_access_key: str = None,
        region_name: str = None,
        profile_name: str = None,
        compression: str | Transform | None = NO_COMPRESSION,
        transform: Transform | None = None,
    ) -> None:
        """Initialize the base attributes of the S3 filesystem for read and write operations.

        Args:
            bucket_name: Name of the bucket with all objects available as files in the filesystem.
            access_key_id: Access key ID with permission to read/write to the bucket.
            secret_access_key: AWS secret access key with permission to read/write to the bucket.
            region_name: Default region when creating bucket connection.
            profile_name: Name of a custom profile to use, instead of default.
            compression: Default compression type to use when reading or writing file contents.
            transform: Default transformation used when reading or writing file contents.
        """
        super().__init__(S3BotoFile, compression=compression, transform=transform)
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


class S3BotoFile(File[S3BotoFilesystem]):
    """File-like object in a remote S3 bucket."""

    __slots__ = ()

    @override
    def __repr__(self) -> str:
        return f"{self.filesystem.bucket_name}:{self.file}"

    @override
    def _read(self) -> bytes:
        try:
            read_response = self.filesystem.client.get_object(Bucket=self.filesystem.bucket_name, Key=self.file)
            content = read_response["Body"].read()
        except self.filesystem.ClientError as client_error:
            # For consistency across File types, change missing objects errors to standard FileNotFoundErrors.
            if client_error.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(errno.ENOENT, f"No such file: {self}") from client_error
            raise client_error
        return content

    @override
    def _write(self, data: bytes) -> int:
        self.filesystem.client.put_object(Body=data, Bucket=self.filesystem.bucket_name, Key=self.file)
        return len(data)


class SQLiteFilesystem(Filesystem):
    """Collection of file-like objects available in a database using SQLite."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        database: str = ":memory:",
        table_name: str = "files",
        file_col: str = "file",
        content_col: str = "content",
        compression: str | Transform | None = NO_COMPRESSION,
        transform: Transform | None = None,
    ) -> None:
        """Initialize the base attributes of the database filesystem for read and write operations.

        Args:
            database: The path to the database file to be opened. e.g., "example.db", ":memory:", etc.
            table_name: Name of the table with data available as files in the filesystem.
            file_col: Name of the column in the table that contains the path to the files.
            content_col: Name of the column in the table that contains the raw contents for the files.
            compression: Default compression type to use when reading or writing file contents.
            transform: Default transformation used when reading or writing file contents.
        """
        for name, value in (
            ("table_name", table_name),
            ("file_col", file_col),
            ("content_col", content_col),
        ):
            if not re.match(r"^[A-za-z0-9_]+$", value):
                raise ValueError(f"{name} may only contain letters, numbers, and underscores.")
        super().__init__(SQLiteFile, compression=compression, transform=transform)

        # Save the database and table name to allow string representations in files,
        # but cache the templates to prevent modifications from impacting later execution.
        self.database = database
        self.table_name = table_name
        self._connection = None
        self._cursor = None
        self._connect()

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

        if self.database == ":memory:":
            self.create_table()

    def commit(self) -> None:
        """Commit any pending transactions to the database backend."""
        self._connection.commit()

    def _connect(self) -> None:
        """Establish a connection to the database, and request a cursor."""
        try:
            # pylint: disable=import-outside-toplevel,redefined-outer-name,reimported
            import sqlite3

        except ModuleNotFoundError as error:
            raise ModuleNotFoundError(f"sqlite3 is required to use {self.__class__.__name__}") from error
        self._connection = sqlite3.connect(self.database)
        self._cursor = self._connection.cursor()

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


class SQLiteFile(File[SQLiteFilesystem]):
    """File-like object in a SQLite database."""

    __slots__ = ()

    @override
    def __repr__(self) -> str:
        return f"sqlite3://{self.filesystem.database}?table_name={self.filesystem.table_name}&file={self.file}"

    @override
    def _read(self) -> bytes:
        res = self.filesystem.execute(self.filesystem.read_query, (self.file,)).fetchone()
        if res is None:
            raise FileNotFoundError(errno.ENOENT, f"No such file: '{self.file}'")
        content = res[0]
        return content

    @override
    def _write(self, data: bytes) -> int:
        self.filesystem.execute(self.filesystem.write_query, (self.file, data, data))
        self.filesystem.commit()
        return len(data)


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
            mod = importlib.import_module(module_name)
            if name == "zstd":
                __COMPRESSORS__[name] = Transform(mod.ZstdCompressor().compress, mod.ZstdDecompressor().decompress)
            else:
                __COMPRESSORS__[name] = Compressor(mod)
        except ImportError:
            pass
    return sorted(set(str(key).lower() for key in __COMPRESSORS__))
