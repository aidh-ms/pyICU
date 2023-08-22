# pyICU

Toolbox for working with data in common medical ICU databases.

## Testing

```bash
PYTHONPATH=".:${PYTHONPATH}" python -m unittest discover
```

## Usage

Using with eICU datbase:

```python
# create a DB connector handing over the engine
db_con = SQLDBConnector(engine)

# create a data loader
loader = eICUDataLoader(db_con)
# The loader can be used to load data from the database. It automatically collects some general admission data on each patient in the database, stored in a pandas dataframe.
```

## Data Model for Items

We aim to provide a fully normalized data model for the items in all available ICU databases. The entire toolbox relies on this data model and each databases is queried by it. The model loaders and connectors all use the data model to provide a unified interface to the data. The data model is publicly available so that it can be used to query any database and can be extended to fit your own database as needed.

```json
{
    "item_identifier": {
        "label": string,
        "specimen": string,
        "units": string,
        "description": string,
        "tags": [
            string
        ],
        "db_settings": [
            {
                "db_schema": {
                    "db_table": [
                            "item_ids": [
                                00000
                            ]
                    ]
                }
            }
        ]
    }
}
``````

This model ensures both extendability and representation of each item by multiple IDs accross different tables and schemas, as it is not uncommon.

* `item_identifier`: The identifier for the item. This is the identifier that is used to itentify the item. It is a string.
* `label`: The label for the item. This is the label that is used to display the item to the user. It is a string.
* `specimen`: The specimen for the item. This is the specimen that the item is measured from. It is a string.
* `units`: The units for the item. This is the units that the item is measured in. It is a string.
* `description`: The description for the item. This is the description that is used to describe the item to the user. It is a string.
* `tags`: The tags for the item. This is a list of tags that are used to describe the item to the user. It is a list of strings.
* `db_settings`: The settings for the item in each database. This is a list of database settings.
* `db_schema`: The schema for the item in a database. This is a dictionary containing datase schemas.
* `db_table`: The table for the item in a database. This is a dictionary containing attributes of the database table.
* `item_ids`: The IDs for the item in a database. This is a list of IDs for the item in a database.

Below is an example representation of the item `creatinine`

```json
{
    "creatinine": {
        "label": "creatinine",
        "specimen": "serum",
        "units": "mg/dL",
        "mimic_settings": [
            {
                "mimiciv_hosp": {
                    "chartevents": {
                        "item_ids": [
                            50912
                        ]
                    }
                }
            }
        ]
    }
}
``````
