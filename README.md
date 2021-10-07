# molgenis-py-cohorts-etl
Python lib containing scripts to control data flow from staging env to live catalogue for cohort rich or descriptive metadata. 

## build
build using venv, activate enviroment ,install requirements;
```pip install -r requirements.txt```

## env vars 

```
MG_CATALOGUE_URL (server location, for example 'https://mycatalog.com')
MG_ETL_USERNAME (default ='admin')
MG_ETL_PASSWORD
MG_SYNC_SOURCES ( comma separated list of sources (schema names) too sync)
MG_SYNC_TARGET ( database name of target schema)
```

## run script 
``` python run.py ```
