from jobFillStaging import JobFillStaging
from decouple import config
import os
import logging

def main():
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
    log = logging.getLogger('run')

    STAGING_URL = config('MG_STAGING_URL', default='https://data-catalogue-staging-test.molgeniscloud.org/')
    CATALOGUE_URL = config('MG_CATALOGUE_URL', default='https://data-catalogue.molgeniscloud.org')
    CATALOGUE_USERNAME = config('MG_CATALOGUE_USERNAME', default='admin')
    CATALOGUE_PASSWORD = config('MG_CATALOGUE_PASSWORD')
    STAGING_USERNAME = config('MG_STAGING_USERNAME', default='admin')
    STAGING_PASSWORD = config('MG_STAGING_PASSWORD')
    SYNC_SOURCE = config('MG_SYNC_SOURCE', default='catalogue')
    SYNC_TARGETS = config('MG_SYNC_TARGETS')

    targets = map(str.strip, SYNC_TARGETS.split(','))

    log.info('*** START SYNC JOB WITH settings: ***')
    log.info('STAGING_USERNAME: ' + STAGING_USERNAME)
    log.info('STAGING_PASSWORD: ********')
    log.info('STAGING_URL: ' + STAGING_URL)
    log.info('CATALOGUE_USERNAME: ' + CATALOGUE_USERNAME)
    log.info('CATALOGUE_PASSWORD: ********')
    log.info('CATALOGUE_URL: ' + CATALOGUE_URL)
    log.info('SYNC_SOURCE: ' + SYNC_SOURCE)
    log.info('SYNC_TARGETS: ' + SYNC_TARGETS)

    for target in targets:
        log.info('START SYNC CATALOGUE (' + SYNC_SOURCE + ') WITH STAGING (' + target + ')')
        job = JobFillStaging(staging_url=STAGING_URL, staging_email=STAGING_USERNAME, staging_password=STAGING_PASSWORD, catalogue_url=CATALOGUE_URL, catalogue_email=CATALOGUE_USERNAME, catalogue_password=CATALOGUE_PASSWORD, catalogueDB=SYNC_SOURCE, targetDB=target)
        log.info('END SYNC CATALOGUE (' + target + ') WITH STAGING (' + SYNC_SOURCE + ')')

    log.info('*** JOB COMPLETED ***')

if __name__ == ("__main__"):
    main()