from pyICU.connection.key import mimic_engine
from pyICU.connection.Connector import SQLDBConnector


def test_connection():
    """Test connection to database and concept loading."""
    connector = SQLDBConnector(mimic_engine)
    print(connector.concepts)


def test_mimic_connector():
    """Test connection to mimic database and concept loading."""
    connector = SQLDBConnector(mimic_engine)
    print(connector.concepts)


if __name__ == '__main__':
    test_mimic_connector()
