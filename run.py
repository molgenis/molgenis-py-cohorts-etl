from client import Client
from decouple import config

# Staging server details
CATALOG_URL = config('MG_CATALOGUE_URL', default='https://catalog-acc.molgeniscloud.org')
ETL_USERNAME = config('MG_ETL_USERNAME', default='admin')
ETL_PASSWORD = config('MG_ETL_PASSWORD')

cohort_a_db = 'cohort a'


# # sign in to staging server
print('Sign in to staging server.')
session_a = Client(url=CATALOG_URL, database=cohort_a_db, email=ETL_USERNAME, password=ETL_PASSWORD)

includedPids = ["co1"]

cohorts = """
    query Cohorts($filter:CohortsFilter) {
        Cohorts (filter:$filter){
            pid
            name
            localName
            acronym
        }
    }      
 """

variables = {'filter': {'pid': {'like': includedPids}}, 'orderby': {}}

r = session_a.query(cohorts, variables)
print(r)

dr = session_a.delete(table='AgeGroups', keyColumn='name', key='test')
print(dr)
