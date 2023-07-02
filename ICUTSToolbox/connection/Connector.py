from abc import ABC, abstractmethod

import pandas as pd
import os
from typing import Optional, Dict
from sqlalchemy import engine
import logging
from ..logger import CustomLogger


logger: logging.Logger = CustomLogger().get_logger()


class DatabaseConnector(ABC):
    """Abstract class for database connectors"""

    def __init__(self, engine: engine):
        self.sqla_engine = engine

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a SQL query and return the result as a DataFrame"""
        raise NotImplementedError

    @abstractmethod
    def execute_query_file(self, file_path: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a SQL query and return the result as a DataFrame"""
        raise NotImplementedError


class SQLDBConnector(DatabaseConnector):

    def __init__(self, engine: engine):
        super().__init__(engine)

    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a SQL query and return the result as a DataFrame"""
        logger.info(f"Executing query: {query}")
        return pd.read_sql(query, self.sqla_engine, params=params)

    def execute_query_file(self, file_path: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a SQL query and return the result as a DataFrame"""
        with open(file_path, 'r') as f:
            query: str = f.read()
        logger.info(f"Executing query: {query}, from file: {file_path}")
        return self.execute_sql(query, params=params)

    def execute_query_folder(self, folder_path: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute all SQL queries in a folder and return the result as a DataFrame"""
        logger.info(f"Executing all queries in folder: {folder_path}")
        return pd.concat([self.execute_query_file(f"{folder_path}/{file}", params=params) for file in os.listdir(folder_path)])
