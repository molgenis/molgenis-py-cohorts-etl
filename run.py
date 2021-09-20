from job import Job
from client import Client
from decouple import config

# Sync script details
CATALOG_URL = config('MG_CATALOGUE_URL', default='https://catalog-acc.molgeniscloud.org')
ETL_USERNAME = config('MG_ETL_USERNAME', default='admin')
ETL_PASSWORD = config('MG_ETL_PASSWORD')

##### ##################
# For cohort a 

cohortA = Job(
    url=CATALOG_URL,
    email=ETL_USERNAME,
    password=ETL_PASSWORD,
    stagingDB='cohort a',
    catalogueDB='UMCG'
)

cohortA.sync()






