from pyICU.connection.key import mimic_demo_engine
from pyICU.connection.Connector import SQLDBConnector, MimicConnector


def test_connection():
    """Test connection to database and concept loading."""
    connector = SQLDBConnector(mimic_demo_engine)
    print(connector.concepts)


def test_mimic_connector():
    """Test connection to mimic database and concept loading."""
    connector = MimicConnector(mimic_demo_engine)
    print(connector.concepts)
    print(connector.execute_query(
        "SELECT * FROM mimiciv_derived.icustay_detail LIMIT 10;"))


if __name__ == '__main__':
    test_mimic_connector()
