from jobFillStaging import JobFillStaging
from decouple import config
import os
import logging

def main():
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
    log = logging.getLogger('run')

    TARGET_URL = config('MG_TARGET_URL', default='https://data-catalogue-staging-test.molgeniscloud.org/')
    SOURCE_URL = config('MG_SOURCE_URL', default='https://data-catalogue.molgeniscloud.org')
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
    log.info('TARGET_URL: ' + TARGET_URL)
    log.info('CATALOGUE_USERNAME: ' + CATALOGUE_USERNAME)
    log.info('CATALOGUE_PASSWORD: ********')
    log.info('SOURCE_URL: ' + SOURCE_URL)
    log.info('SYNC_SOURCE: ' + SYNC_SOURCE)
    log.info('SYNC_TARGETS: ' + SYNC_TARGETS)

    for target in targets:
        log.info('START SYNC CATALOGUE (' + SYNC_SOURCE + ') WITH STAGING (' + target + ')')
        #job = JobFillStaging(
        # target_url=target_URL, staging_email=STAGING_USERNAME, staging_password=STAGING_PASSWORD, target_url=target_URL, catalogue_email=CATALOGUE_USERNAME, catalogue_password=CATALOGUE_PASSWORD, catalogueDB=SYNC_SOURCE, targetDB=target)
        log.info('END SYNC CATALOGUE (' + target + ') WITH TARGET (' + SYNC_SOURCE + ')')

    log.info('*** JOB COMPLETED ***')

if __name__ == ("__main__"):
    main()