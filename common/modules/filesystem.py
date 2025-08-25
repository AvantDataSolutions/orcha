import io
from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd
from pandas import DataFrame
from smb.SMBConnection import SMBConnection
from smb.SMBConnection import OperationFailure

from orcha.core.module_base import EntityBase, SinkBase, SourceBase


class FileSystemEntity(EntityBase, ABC):
    """
    Generic entity class to handle file operations for reading and writing files
    to a file system. Abstract class to be implemented by specific file system entities.
    """
    def __init__(self,
                 module_idk: str, description: str,
                 folder: str, user_name: str, password: str):
        super().__init__(
            module_idk=module_idk,
            description=description,
            user_name=user_name,
            password=password
        )
        self.folder = folder

    @abstractmethod
    def to_csv(self, file_name: str, df: pd.DataFrame):
        """
        Abstract method to write a DataFrame to a CSV file
        """
        raise NotImplementedError

    @abstractmethod
    def to_excel(self, file_name: str, df: pd.DataFrame):
        """
        Abstract method to write a DataFrame to an Excel file
        """
        raise NotImplementedError

    @abstractmethod
    def from_csv(self, file_name: str) -> pd.DataFrame:
        """
        Abstract method to read a DataFrame from a CSV file
        """
        raise NotImplementedError

    @abstractmethod
    def from_excel(self, file_name: str) -> pd.DataFrame:
        """
        Abstract method to read a DataFrame from an Excel file
        """
        raise NotImplementedError


class SmbEntity(FileSystemEntity):
    """
    An entity class to handle the SMB connection and file operations
    for reading and writing files to an SMB share
    """
    def __init__(self,
            module_idk: str, description: str,
            host: str, share: str, folder: str,
            user_name: str, password: str
        ):
        """
        Creates an instance of the SMB entity with appropriate credentials
        #### Parameters
        - host (str): IP address or hostname of the SMB server
        - share (str): Share name on the SMB server
        - folder (str): Folder path within the share
        - user_name (str): user_name for authentication
        - password (str): Password for authentication
        """
        super().__init__(
            module_idk=module_idk,
            description=description,
            folder=folder,
            user_name=user_name,
            password=password
        )
        self.host = host
        self.share = share

    def _to_file(self, file_name: str, df: pd.DataFrame, file_format: str):
        """
        Writes a DataFrame to a file on the SMB share generalised here
        to handle both CSV and Excel file formats
        """
        client = SMBConnection(
            username=self.user_name,
            password=self.password,
            my_name=self.user_name,
            remote_name=self.host,
            is_direct_tcp=True
        )

        file_path = f'{self.folder}/{file_name}'
        _is_connected = client.connect(self.host, 445)
        if not _is_connected:
            raise ConnectionError(f'Connection failed for {self.host}')
        if not client.has_authenticated:
            raise ConnectionError(f'Authentication failed for {self.user_name}')
        with io.BytesIO() as file_obj:
            try:
                if file_format == 'csv':
                    df.to_csv(file_obj, index=False)
                elif file_format == 'excel':
                    df.to_excel(file_obj, index=False)
                else:
                    raise ValueError(f'Unsupported file format: {file_format}')
                file_obj.seek(0)
                client.storeFile(self.share, file_path, file_obj)
            except Exception as e:
                client.close()
                raise e

    def to_csv(self, file_name: str, df: pd.DataFrame):
        """
        SMB specific implementation to write a DataFrame to a CSV file
        #### Parameters
        - file_name (str): Name must be compliant with SMB file naming conventions
        - df (pd.DataFrame): DataFrame to be written to the file
        """
        self._to_file(file_name, df, 'csv')

    def to_excel(self, file_name: str, df: pd.DataFrame):
        """
        SMB specific implementation to write a DataFrame to an Excel file
        #### Parameters
        - file_name (str): Name must be compliant with SMB file naming conventions
        - df (pd.DataFrame): DataFrame to be written to the file
        """
        self._to_file(file_name, df, 'excel')

    def _from_file(self, file_name: str, file_format: str) -> pd.DataFrame:
        """
        Reads a DataFrame from a file on the SMB share generalised here
        to handle both CSV and Excel file formats
        """
        client = SMBConnection(
            username=self.user_name,
            password=self.password,
            my_name=self.user_name,
            remote_name=self.host,
            is_direct_tcp=True
        )
        file_path = f'{self.folder}/{file_name}'
        _is_connected = client.connect(self.host, 445)
        if not _is_connected:
            raise ConnectionError(f'Connection failed for {self.host}')
        if not client.has_authenticated:
            raise ConnectionError(f"Authentication failed for {self.user_name}")
        with io.BytesIO() as file_obj:
            try:
                client.retrieveFile(self.share, file_path, file_obj)
                file_obj.seek(0)

                if file_format == 'csv':
                    df = pd.read_csv(file_obj)
                elif file_format == 'excel':
                    df = pd.read_excel(file_obj)
                else:
                    raise ValueError(f'Unsupported file format: {file_format}')
                return df
            except OperationFailure as e:
                if '0xC0000034' in str(e):
                    # Only for file not found errors are we returning an empty DataFrame
                    # If it's another 'can't read file' error, we want to raise it
                    return pd.DataFrame()
                client.close()
                raise e

    def from_csv(self, file_name: str) -> pd.DataFrame:
        """
        SMB specific implementation to read a DataFrame from a CSV file
        #### Parameters
        - file_name (str): Name must be compliant with SMB file naming conventions
        #### Returns
        - pd.DataFrame: DataFrame read from the file
        """
        return self._from_file(file_name, 'csv')

    def from_excel(self, file_name: str) -> pd.DataFrame:
        """
        SMB specific implementation to read a DataFrame from an Excel file
        #### Parameters
        - file_name (str): Name must be compliant with SMB file naming conventions
        #### Returns
        - pd.DataFrame: DataFrame read from the file
        """
        return self._from_file(file_name, 'excel')


@dataclass
class FileSystemSink(SinkBase):
    """
    Sink class for writing to a file system using the data entity
    to handle the writing logic
    """
    data_entity: FileSystemEntity
    file_name: str


@dataclass
class FileSystemSource(SourceBase):
    """
    Source class for reading from a file system using the data entity
    to handle the reading logic
    """
    data_entity: FileSystemEntity
    file_name: str


@dataclass
class CsvSink(FileSystemSink):
    """
    Sink class for writing data to a CSV file
    """

    def save(self, data: DataFrame) -> None:
        """
        Saves a DataFrame to a CSV file using the data entity
        """
        self.data_entity.to_csv(self.file_name, data)


@dataclass
class CsvSource(FileSystemSource):
    """
    Source class for reading data from a CSV file
    """
    data_entity: FileSystemEntity
    file_name: str

    def get(self) -> DataFrame:
        """
        Reads a DataFrame from a CSV file using the data entity
        """
        return self.data_entity.from_csv(self.file_name)


@dataclass
class ExcelSink(FileSystemSink):
    """
    Sink class for writing data to an Excel file
    """

    def save(self, data: DataFrame) -> None:
        """
        Saves a DataFrame to an Excel file using the data entity
        #### Parameters
        - data: DataFrame
        """
        self.data_entity.to_excel(self.file_name, data)


@dataclass
class ExcelSource(FileSystemSource):
    """
    Source class for reading data from an Excel file
    """
    def get(self) -> DataFrame:
        """
        Reads a DataFrame from an Excel file using the data entity
        """
        return self.data_entity.from_excel(self.file_name)