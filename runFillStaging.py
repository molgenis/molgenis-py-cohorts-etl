from jobFillStaging import JobFillStaging
from decouple import config
import os
import logging

def main():
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
    log = logging.getLogger('run')

    SOURCE_URL = config('MG_SOURCE_URL', default='https://data-catalogue.molgeniscloud.org')
    SOURCE_USERNAME = config('MG_SOURCE_USERNAME', default='admin')
    SOURCE_PASSWORD = config('MG_SOURCE_PASSWORD')
    
    TARGET_URL = config('MG_TARGET_URL', default='https://data-catalogue-staging-test.molgeniscloud.org/')
    TARGET_USERNAME = config('MG_TARGET_USERNAME', default='admin')
    TARGET_PASSWORD = config('MG_TARGET_PASSWORD')
    
    SYNC_SOURCE = config('MG_SYNC_SOURCE', default='catalogue')
    SYNC_TARGETS = config('MG_SYNC_TARGETS')

    targets = map(str.strip, SYNC_TARGETS.split(','))

    log.info('*** START SYNC JOB WITH settings: ***')
    log.info('TARGET_URL: ' + TARGET_URL)
    log.info('TARGET_USERNAME: ' + TARGET_USERNAME)
    log.info('TARGET_PASSWORD: ********')
    log.info('SOURCE_URL: ' + SOURCE_URL)
    log.info('SOURCE_USERNAME: ' + SOURCE_USERNAME)
    log.info('SOURCE_PASSWORD: ********')
    
    log.info('SYNC_SOURCE: ' + SYNC_SOURCE)
    log.info('SYNC_TARGETS: ' + SYNC_TARGETS)

    for target in targets:
        log.info('START SYNC SOURCE (' + SYNC_SOURCE + ') WITH TARGET (' + target + ')')
        job = JobFillStaging(
            target_url=TARGET_URL,
            target_email=TARGET_USERNAME,
            target_password=TARGET_PASSWORD,
            source_url=SOURCE_URL,
            source_email=SOURCE_USERNAME,
            source_password=SOURCE_PASSWORD,
            catalogueDB=SYNC_SOURCE,
            targetDB=target
        )
        log.info('END SYNC SOURCE (' + target + ') WITH TARGET (' + SYNC_SOURCE + ')')

    log.info('*** JOB COMPLETED ***')

if __name__ == ("__main__"):
    main()