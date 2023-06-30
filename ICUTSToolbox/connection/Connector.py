from abc import ABC, abstractmethod

import pandas as pd

from typing import Optional, Dict


class DatabaseConnector(ABC):
    """Abstract class for database connectors"""

    def __init__(self, engine: engine):
        self.sqla_engine = engine

    @abstractmethod
    def execute_sql(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a SQL query fand return the result as a DataFrame"""
        pass
