from abc import ABC, abstractmethod
from connection import SQLDBConnector


class DataLoader(ABC):
    """Abstract class for data loaders"""

    def __init__(self, *args, **kwargs):
        self._db_connector = None
        self._admission_data = None
        self._stay_identifier = None
        self._patient_identifier = None
        self._admission_identifier = None
        self._time_identifier = None

    @abstractmethod
    def load_data(self, *args, **kwargs):
        """Load data from a source"""
        raise NotImplementedError

    @abstractmethod
    def get_admission_data(self):
        """Return the admission data"""
        raise NotImplementedError


class eICUDataLoader(DataLoader):
    """Class for loading data from the eICU database"""

    def __init__(self, db_connector: SQLDBConnector,  *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._db_connector: SQLDBConnector = db_connector

    def load_data(self, *args, **kwargs):
        """Load data from the eICU database"""
        pass

    def get_admission_data(self):
        """Return the admission data"""
