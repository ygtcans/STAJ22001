import pandas as pd
from abc import ABC, abstractmethod
import json
import os
import datetime
from src.db_connections import PostgreSQLDB
from sqlalchemy import text

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
        
class PostgresDataHandler(BaseDataHandler):
    """Handles reading and writing data to a PostgreSQL database."""

    def _init_(self):
        self.postgres = PostgreSQLDB()
        self.engine, self.session = self.postgres.connect()

    def _get_postgres_type(self, series: pd.Series) -> str:
        """
        Maps a Pandas Series dtype to a PostgreSQL data type.

        Args:
            series (pd.Series): The Pandas Series to map.

        Returns:
            str: The PostgreSQL data type.
        """
        type_mapping = {
            'object': 'TEXT',
            'int64': 'BIGINT',
            'int32': 'INT',
            'float64': 'DOUBLE PRECISION',
            'float32': 'FLOAT',
            'datetime64[ns]': 'TIMESTAMP'
        }

        series_dtype = str(series.dtype)
        return type_mapping.get(series_dtype, 'TEXT')


    def _create_table(self, table_name: str, columns_info: str) -> None:
        """
        Creates a PostgreSQL table if it does not exist.

        Args:
            table_name (str): The name of the table.
            columns_info (str): The columns and their data types.

        Returns:
            None
        """
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_info})"
        try:
            self.postgres.execute_query(text(create_table_query))
            print(f"Table created successfully: {table_name}")
        except Exception as e:
            raise RuntimeError(f"Failed to create table {table_name}: {e}")

    def write(self, data: pd.DataFrame, table_name: str) -> None:
        """
        Writes data to a PostgreSQL table.

        Args:
            data (pd.DataFrame): The DataFrame to write.
            table_name (str): The name of the PostgreSQL table.

        Returns:
            None
        """
        try:
            column_types = {column: self._get_postgres_type(data[column]) for column in data.columns}
            columns_info = ', '.join([f"{col} {col_type}" for col, col_type in column_types.items()])
            self._create_table(table_name, columns_info)
            data.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            print(f"Successfully written DataFrame to PostgreSQL table: {table_name}")
        except Exception as e:
            raise RuntimeError(f"Failed to write DataFrame to PostgreSQL table {table_name}: {e}")
        finally:
            self.postgres.disconnect()

    def read(self, table_name: str) -> pd.DataFrame:
        """
        Reads data from a PostgreSQL table.

        Args:
            table_name (str): The name of the table to read.

        Returns:
            pd.DataFrame: The DataFrame containing the read data.
        """
        try:
            query = f"SELECT * FROM {table_name}"
            result = self.postgres.execute_query(query)
            data = result.fetchall()
            columns = result.keys()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            raise RuntimeError(f"Failed to read data from PostgreSQL table {table_name}: {e}")