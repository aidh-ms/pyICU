# ICUTSToolbox

Toolbox for working with time series data in common medical ICU databases.

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
