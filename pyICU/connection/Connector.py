"""Module for database connectors"""

import os
import logging
import json

import pandas as pd
from abc import ABC, abstractmethod
from typing import Optional, Dict, List
from sqlalchemy import engine, text

from ..logger import CustomLogger


logger: logging.Logger = CustomLogger().get_logger()


class DatabaseConnector(ABC):
    """Abstract class for database connectors"""

    def __init__(self, conn_engine: engine):
        self.sqla_engine = conn_engine

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a SQL query and return the result as a DataFrame"""
        raise NotImplementedError

    @abstractmethod
    def execute_query_file(self, file_path: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a SQL query and return the result as a DataFrame"""
        raise NotImplementedError


class SQLDBConnector(DatabaseConnector):
    """
    Connector for SQL databases.

    This class is used to connect to a SQL database and execute queries.

    Parameters
    ----------
    DatabaseConnector : DatabaseConnector
        Abstract class for database connectors
    """

    def __init__(self, conn_engine: engine):
        super().__init__(conn_engine)
        self.concepts = self.load_concepts()

    def load_concepts(self, json_file: str = 'concepts.json') -> Dict:
        """Load concepts from the concepts.json file."""
        # get json file path, located in the pyICU main directory which is the parent of the current directory
        json_path = os.path.join(os.path.dirname(
            os.path.abspath(__file__)), '..', json_file)
        logger.info("Loading concepts from %s", json_path)
        concepts = json.load(open(json_path, 'r'))
        return concepts

    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute an SQL query and return the result as a DataFrame"""
        logger.info("Executing query: %s", query)
        return pd.read_sql(text(query), self.sqla_engine, params=params)

    def execute_query_file(self, file_path: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute an SQL query and return the result as a DataFrame"""
        with open(file_path, 'r') as f:
            query: str = f.read()
        logger.info("Executing query from filepath %s", file_path)
        return self.execute_query(query, params=params)

    def execute_query_folder(self, folder_path: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute all SQL queries in a folder and return the result as a DataFrame"""
        logger.info("Executing queries from folder %s", folder_path)
        return pd.concat([self.execute_query_file(f"{folder_path}/{file}", params=params) for file in os.listdir(folder_path)])

    def get_concept(self, concept_name: str) -> Dict:
        """Get a concept from the concepts.json file."""
        return self.concepts[concept_name]

    def get_full_set_for_concept(self, item: str) -> pd.DataFrame:
        """Get the full set of data for an item from a database."""
        raise NotImplementedError

    def get_concept_for_patient(self, item: str, patient_id: str) -> pd.DataFrame:
        """Get the data for an item for a specific patient from a database."""
        raise NotImplementedError


class MimicConnector(SQLDBConnector):
    """Connector for the MIMIC Database."""

    def __init__(self, conn_engine: engine):
        super().__init__(conn_engine)

    def get_full_set_for_concept(self, item: str) -> pd.DataFrame:
        """Get the full set of data for an item from a database."""
        concept: Dict = self.get_concept(item)
        settings: List = concept["mimic_settings"]
        for schema in settings:
            print(schema)
