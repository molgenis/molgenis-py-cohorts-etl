import job
from decouple import config
import os
import logging

def main():
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
    log = logging.getLogger('run')

    SOURCE_URL = config('MG_SOURCE_URL', default='https://data-catalogue.molgeniscloud.org')
    SOURCE_USERNAME = config('MG_SOURCE_USERNAME', default='admin')
    SOURCE_PASSWORD = config('MG_SOURCE_PASSWORD')
    SOURCE_DATABASE = config('MG_SOURCE_DATABASE', default='catalogue')
    
    TARGET_URL = config('MG_TARGET_URL', default='https://data-catalogue-staging-test.molgeniscloud.org/')
    TARGET_USERNAME = config('MG_TARGET_USERNAME', default='admin')
    TARGET_PASSWORD = config('MG_TARGET_PASSWORD')
    TARGET_DATABASE = config('MG_TARGET_DATABASE')

    JOB_STRATEGY = config('MG_JOB_STRATEGY')

    log.info('*** START SYNC JOB WITH settings: ***')
    log.info('JOB_STRATEGY: ' + JOB_STRATEGY)
    log.info('TARGET_URL: ' + TARGET_URL)
    log.info('TARGET_USERNAME: ' + TARGET_USERNAME)
    log.info('TARGET_PASSWORD: ********')
    log.info('TARGET_DATABASE(S): ' + TARGET_DATABASE)

    log.info('SOURCE_URL: ' + SOURCE_URL)
    log.info('SOURCE_USERNAME: ' + SOURCE_USERNAME)
    log.info('SOURCE_PASSWORD: ********')
    log.info('SOURCE_DATABASE: ' + SOURCE_DATABASE)


    targets = map(str.strip, TARGET_DATABASE.split(','))

    for target in targets:
        log.info('START SYNC SOURCE (' + SOURCE_DATABASE + ') WITH TARGET (' + target + ')')
        job.Job(
            target_url = TARGET_URL,
            target_email = TARGET_USERNAME,
            target_password = TARGET_PASSWORD,
            target_database = target,
            source_url = SOURCE_URL,
            source_email = SOURCE_USERNAME,
            source_password = SOURCE_PASSWORD,
            source_database = SOURCE_DATABASE,
            job_strategy = JOB_STRATEGY,
        )
        log.info('END SYNC SOURCE (' + SOURCE_DATABASE + ') WITH TARGET (' + target + ')')

    log.info('*** JOB COMPLETED ***')

if __name__ == ("__main__"):
    main()