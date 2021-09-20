from client import Client
from decouple import config
from pathlib import Path

# Sync script details
CATALOG_URL = config('MG_CATALOGUE_URL', default='https://catalog-acc.molgeniscloud.org')
ETL_USERNAME = config('MG_ETL_USERNAME', default='admin')
ETL_PASSWORD = config('MG_ETL_PASSWORD')

##### ##################
# For cohort a 
print('*** START SYNC STAGING WITH CATALOGUE ***')
staging = Client(url=CATALOG_URL, database='cohort a', email=ETL_USERNAME, password=ETL_PASSWORD)
catalogue = Client(url=CATALOG_URL, database='UMCG', email=ETL_USERNAME, password=ETL_PASSWORD)

# query from staging 
cohorts = Path('cohorts.gql').read_text()
variables = {'filter': {'name': {'like': 'cohort a'}}}
cohortsResp = staging.query(cohorts, variables)
cohorts = []
if "Cohorts" in cohortsResp:
    cohorts = cohortsResp['Cohorts']
pids = list(map(lambda cohort: cohort['pid'], cohorts))

# delete from catalog
print('cohorts to delete: ' + ",".join(pids))
for pid in pids:
    dr = catalogue.delete(table='Cohorts', keyColumn='pid', key=pid)

# download from staging
newData = staging.downLoadCSV('Cohorts')

# upload to catalog
r = catalogue.uploadCSV('Cohorts', newData)

print('*** END SYNC STAGING WITH CATALOGUE ***')
# END SYNC
##########################






