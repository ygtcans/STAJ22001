import pandas as pd
from abc import ABC, abstractmethod
import json
import os
import datetime

class BaseDataHandler(ABC):

    @abstractmethod
    def read(self) -> pd.DataFrame:
        """Abstract method to read data from a source."""
        pass

    @abstractmethod
    def write(self, data: pd.DataFrame) -> None:
        """Abstract method to write data to a destination."""
        pass

class LocalDataHandler(BaseDataHandler):
    """Handles reading and writing local data files."""

    def read(self, file_path: str, extension: str = None) -> pd.DataFrame:
        """
        Reads data from a local file.

        Args:
            file_path (str): The path of the file to read.
            extension (str): The file extension (if not in the file name).

        Returns:
            pd.DataFrame: The DataFrame containing the read data.
        """
        readers = {
            'json': lambda path: pd.DataFrame(json.load(open(path, 'r'))),
            'csv': pd.read_csv,
            'parquet': pd.read_parquet
        }

        file_extension = extension or file_path.split('.')[-1]

        reader_func = readers.get(file_extension)
        if reader_func:
            try:
                return reader_func(file_path)
            except Exception as e:
                raise IOError(f"Error reading file at {file_path}: {e}")
        else:
            raise ValueError(f"Unsupported file extension: {file_extension}")

    def _generate_file_name(self, base_name: str, extension: str, output_dir: str) -> str:
        """
        Generates a file name with a timestamp.

        Args:
            base_name (str): The base name of the file.
            extension (str): The file extension.
            output_dir (str): The directory where the file will be saved.

        Returns:
            str: The generated file name.
        """
        date_str = datetime.datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
        return os.path.join(output_dir, f"{base_name}_{date_str}.{extension}")

    def write(self, df: pd.DataFrame, base_name: str, output_dir: str, extension: str) -> None:
        """
        Writes data to a local file.

        Args:
            df (pd.DataFrame): The DataFrame to write.
            base_name (str): The base name of the file.
            output_dir (str): The directory where the file will be saved.
            extension (str): The file extension.

        Returns:
            None
        """
        os.makedirs(output_dir, exist_ok=True)
        file_path = self._generate_file_name(base_name, extension, output_dir)

        writers = {
            'json': lambda path: df.to_json(path),
            'csv': lambda path: df.to_csv(path, index=False),
            'parquet': lambda path: df.to_parquet(path, index=False)
        }

        writer_func = writers.get(extension)
        if writer_func:
            try:
                writer_func(file_path)
                print(f"Data written to {file_path}")
            except Exception as e:
                raise IOError(f"Error writing file to {file_path}: {e}")
        else:
            raise ValueError(f"Unsupported file extension: {extension}")