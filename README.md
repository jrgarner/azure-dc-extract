# DataCatalog Extraction Function

This is a "TimerTrigger" python-based function, that is schedukle to execute daily to extract all entries in 
the DataCatalog table, and exports them to a CSV file for user consumption

# Required parameter/variable values

The following values are required to be provided to the function, via the FunctionApps app_settings values

DC_CONNECTION_STRING: DataCatalog StorageAccount/Table connection string
FS_CONNECTION_STRING: Connection string to the StorageAccount where CSV file is to be stored
DC_TABLE_NAME: DataCatalog table name
LOGGER_LEVEL: Logger level used during function execution
CATALOG_FILENAME: Name of catalog file to be generated (e.g. catalog.csv)
TABLE_FILTER: Filter to apply to DataCatalog table to extract actual DataCatalog entyries (e.g. "PartitionKey eq '*'")
SHARE_NAME: Name of file share where CSV file is stored

