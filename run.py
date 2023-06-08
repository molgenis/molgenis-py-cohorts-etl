import logging
import os
import sys

from dotenv import load_dotenv

from src import Job, JobStrategy
from src.exceptions import InvalidJobStrategyError


def main():
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
    log = logging.getLogger('run')

    # Load the variables from the .env file into the environment
    load_dotenv()

    try:
        source_url = os.environ['MG_SOURCE_URL']
        source_username = os.environ['MG_SOURCE_USERNAME']
        source_password = os.environ['MG_SOURCE_PASSWORD']
        source_database = os.environ['MG_SOURCE_DATABASE']

        target_url = os.environ['MG_TARGET_URL']
        target_username = os.environ['MG_TARGET_USERNAME']
        target_password = os.environ['MG_TARGET_PASSWORD']
        target_database = os.environ['MG_TARGET_DATABASE']

        target_ontology = os.environ['MG_TARGET_ONTOLOGY']

        job_strategy = os.environ['MG_JOB_STRATEGY']
    except KeyError:
        log.error('Make sure you filled in all variables in the .env file, script will exit now.')
        sys.exit()

    if job_strategy not in JobStrategy.member_names():
        raise InvalidJobStrategyError(job_strategy)

    log.info('*** START SYNC JOB WITH settings: ***')
    log.info(f'JOB_STRATEGY:       {job_strategy}')

    log.info(f'TARGET_URL:         {target_url}')
    log.info(f'TARGET_USERNAME:    {target_username}')
    log.info(f'TARGET_PASSWORD:    ********')
    log.info(f'TARGET_DATABASE(S): {target_database}')

    if source_database:
        log.info(f'SOURCE_URL:         {source_url}')
        log.info(f'SOURCE_USERNAME:    {source_username}')
        log.info(f'SOURCE_PASSWORD:    ********')
        log.info(f'SOURCE_DATABASE:    {source_database}')

    targets = map(str.strip, target_database.split(','))
    sources = map(str.strip, source_database.split(','))

    for target in targets:
        if not source_database:
            log.info(f'START SYNC WITH TARGET ({target})')
            this_job = Job(
                target_url=target_url,
                target_email=target_username,
                target_password=target_password,
                target_database=target,
                target_ontology=target_ontology,
                source_url='',
                source_email='',
                source_password='',
                source_database='',
                job_strategy=job_strategy,
            )
            this_job.run_strategy()
            log.info(f'END SYNC WITH TARGET ({target})')
        else:
            for source in sources:
                log.info(f'START SYNC SOURCE ({source}) WITH TARGET ({target})')
                this_job = Job(
                    target_url=target_url,
                    target_email=target_username,
                    target_password=target_password,
                    target_database=target,
                    target_ontology='',
                    source_url=source_url,
                    source_email=source_username,
                    source_password=source_password,
                    source_database=source,
                    job_strategy=job_strategy,
                )
                this_job.run_strategy()
                log.info(f'END SYNC SOURCE ({source}) WITH TARGET ({target})')

    log.info('*** JOB COMPLETED ***')


if __name__ == "__main__":
    main()
