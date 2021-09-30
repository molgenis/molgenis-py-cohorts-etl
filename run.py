from job import Job
from decouple import config

CATALOG_URL = config('MG_CATALOGUE_URL', default='https://catalogue-acc.molgeniscloud.org')
ETL_USERNAME = config('MG_ETL_USERNAME', default='admin')
ETL_PASSWORD = config('MG_ETL_PASSWORD')
SYNC_SOURCES = config('MG_SYNC_SOURCES', default='')
SYNC_TARGET = config('MG_SYNC_TARGET', default='catalogue')

soures = map(str.strip, SYNC_SOURCES.split(','))

print('*** START SYNC JOB WITH settings: ***')
print('ETL_USERNAME: ' + ETL_USERNAME)
print('ETL_PASSWORD: ********')
print('CATALOG_URL: ' + CATALOG_URL)
print('SYNC_SOURCES: ' + SYNC_SOURCES)
print('SYNC_TARGET: ' + SYNC_TARGET)

job = Job(
    url=CATALOG_URL,
    email=ETL_USERNAME,
    password=ETL_PASSWORD,
    catalogueDB=SYNC_TARGET
)

for source in soures:
    print('START SYNC STAGING(' + source + ') WITH CATALOGUE (' + SYNC_TARGET + ')')
    job.sync(sourceDB=source)
    print('END SYNC STAGING(' + source + ') WITH CATALOGUE (' + SYNC_TARGET + ')')

print('*** JOB COMPLETED ***')







