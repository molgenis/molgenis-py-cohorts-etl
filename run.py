from client import Client
from decouple import config
from pathlib import Path

# Staging server details
CATALOG_URL = config('MG_CATALOGUE_URL', default='https://catalog-acc.molgeniscloud.org')
ETL_USERNAME = config('MG_ETL_USERNAME', default='admin')
ETL_PASSWORD = config('MG_ETL_PASSWORD')

cohort_a_db = 'cohort a'


# For cohort a 
print('Sign in to staging server.')
umcg = Client(url=CATALOG_URL, database='UMCG', email=ETL_USERNAME, password=ETL_PASSWORD)

cohorts = Path('cohorts.gql').read_text()

includedPids = ["co1"]

variables = {'filter': {'pid': {'like': includedPids}}}

# delete from catalog
dr = umcg.delete(table='Cohorts', keyColumn='pid', key=includedPids[0])

# query from staging 
cohortA = Client(url=CATALOG_URL, database='cohort a', email=ETL_USERNAME, password=ETL_PASSWORD)
r = cohortA.query(cohorts, variables)

# add to catalog
# addResp = umcg.add('Cohorts', {"pid":"bar2","name":"bar name 2","acronym":"bla bla 2","homepage":"sddss"})
# print(addResp)
