# molgenis-py-cohorts-etl

Python lib containing scripts to control data flow from staging env to live catalogue for cohort rich or descriptive metadata and for variable metadata. Set up of servers and datamodels are found [here](https://github.com/molgenis/molgenis-py-cohorts-etl/datamodels)

## build

build using venv, activate enviroment ,install requirements;
```pip install -r requirements.txt```

## env vars

```
# Set job_strategy (SharedStaging, FillStaging, CohortStagingToDataCatalogue, NetworkStagingToDataCatalogue, UMCGCohorts)
MG_JOB_STRATEGY = CohortStagingToDataCatalogue

# fill out SOURCE URL AND CREDENTIALS
MG_SOURCE_URL = https://data-catalogue-staging-test.molgeniscloud.org
MG_SOURCE_USERNAME = admin
MG_SOURCE_PASSWORD = ...
MG_SOURCE_DATABASE = Test

# fill out TARGET URL AND CREDENTIALS
MG_TARGET_URL = https://data-catalogue-staging-test.molgeniscloud.org
MG_TARGET_USERNAME = admin
MG_TARGET_PASSWORD = ...
MG_TARGET_DATABASE = DataCatalogueTest
```

## run script

``` python run.py ```
