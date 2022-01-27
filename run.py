from jobDataCatalogue import JobDataCatalogueCohorts
from jobDataCatalogue import JobDataCatalogueNetworks
from decouple import config
import os
import logging

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger('run')

CATALOG_URL = config('MG_CATALOGUE_URL', default='https://catalogue-acc.molgeniscloud.org')
ETL_USERNAME = config('MG_ETL_USERNAME', default='admin')
ETL_PASSWORD = config('MG_ETL_PASSWORD')
SYNC_COHORTS = config('MG_SYNC_COHORTS', default='')
SYNC_NETWORKS = config('MG_SYNC_NETWORKS', default='')
SYNC_TARGET = config('MG_SYNC_TARGET', default='catalogue')

cohorts = map(str.strip, SYNC_COHORTS.split(','))
networks = map(str.strip, SYNC_NETWORKS.split(','))

log.info('*** START SYNC JOB WITH settings: ***')
log.info('ETL_USERNAME: ' + ETL_USERNAME)
log.info('ETL_PASSWORD: ********')
log.info('CATALOG_URL: ' + CATALOG_URL)
log.info('SYNC_COHORTS: ' + SYNC_COHORTS)
log.info('SYNC_NETWORKS: ' + SYNC_NETWORKS)
log.info('SYNC_TARGET: ' + SYNC_TARGET)

for cohort in cohorts:
    log.info('START SYNC COHORT STAGING (' + cohort + ') WITH CATALOGUE (' + SYNC_TARGET + ')')
    job = JobDataCatalogueCohorts(url=CATALOG_URL, email=ETL_USERNAME, password=ETL_PASSWORD, catalogueDB=SYNC_TARGET, sourceDB=cohort)
    job.sync()
    log.info('END SYNC COHORT STAGING (' + cohort + ') WITH CATALOGUE (' + SYNC_TARGET + ')')

for network in networks:
    log.info('START SYNC NETWORK STAGING (' + network + ') WITH CATALOGUE (' + SYNC_TARGET + ')')
    job = JobDataCatalogueNetworks(url=CATALOG_URL, email=ETL_USERNAME, password=ETL_PASSWORD, catalogueDB=SYNC_TARGET, sourceDB=network)
    job.sync()
    log.info('END SYNC NETWORK STAGING (' + network + ') WITH CATALOGUE (' + SYNC_TARGET + ')')

log.info('*** JOB COMPLETED ***')
