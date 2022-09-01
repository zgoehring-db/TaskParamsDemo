# Unity Catalog Examples 



## Information Schema 

Please reference `uc_information_schema.py`

The INFORMATION_SCHEMA is a SQL Standard based, system provided schema present in every catalog other than the HIVE_METASTORE catalog.

Within the information schema, you can find a set of views describing the objects known to the schema’s catalog that you are privileged the see. The information schema of the SYSTEM catalog returns information about objects across all catalogs within the metastore.

The purpose of the information schema is to provide a SQL based, self describing API to the metadata.


Below is a sample query than can be used to generate a network graph of the table relationships: 
```sql

```
<br></br>
Visual:
<br></br>
<img src="https://racadlsgen2.blob.core.windows.net/public/TableNetwork.png" width=500 />


