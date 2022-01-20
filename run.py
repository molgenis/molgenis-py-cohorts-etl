from job import Job
from decouple import config
import os
import logging

if __name__ == '__main__':
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
    log = logging.getLogger('run')

    CATALOG_URL = config('MG_CATALOGUE_URL', default='https://catalogue-acc.molgeniscloud.org')
    ETL_USERNAME = config('MG_ETL_USERNAME', default='admin')
    ETL_PASSWORD = config('MG_ETL_PASSWORD')
    SYNC_SOURCES = config('MG_SYNC_SOURCES', default='')
    SYNC_TARGET = config('MG_SYNC_TARGET', default='catalogue')

    soures = map(str.strip, SYNC_SOURCES.split(','))

    log.info('*** START SYNC JOB WITH settings: ***')
    log.info('ETL_USERNAME: ' + ETL_USERNAME)
    log.info('ETL_PASSWORD: ********')
    log.info('CATALOG_URL: ' + CATALOG_URL)
    log.info('SYNC_SOURCES: ' + SYNC_SOURCES)
    log.info('SYNC_TARGET: ' + SYNC_TARGET)

    for source in soures:
        log.info('START SYNC STAGING (' + source + ') WITH CATALOGUE (' + SYNC_TARGET + ')')
        job = Job(url=CATALOG_URL, email=ETL_USERNAME, password=ETL_PASSWORD, catalogueDB=SYNC_TARGET, sourceDB=source)
        job.sync()
        log.info('END SYNC STAGING(' + source + ') WITH CATALOGUE (' + SYNC_TARGET + ')')

    log.info('*** JOB COMPLETED ***')







