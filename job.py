import logging
from enum import Enum, auto

import client
import sync

log = logging.getLogger(__name__)


class Job:
    def __init__(
            self,
            target_url: str,
            target_email: str,
            target_password: str,
            target_database: str,
            source_url: str,
            source_email: str,
            source_password: str,
            source_database: str,
            job_strategy: str) -> None:
        """A Job object consists of data on the source database, the target database
        and the Job strategy.
        """

        # Set up Client for SOURCE
        self.source = client.Client(
            url=source_url,
            database=source_database,
            email=source_email,
            password=source_password
        )

        # Set up Client for TARGET
        self.target = client.Client(
            url=target_url,
            database=target_database,
            email=target_email,
            password=target_password
        )

        self.job_strategy = job_strategy

        # Ensure database schemas exist, otherwise exit
        self.source.check_database_exists()
        self.target.check_database_exists()

    def run_strategy(self) -> None:

        if self.job_strategy == JobStrategy.NETWORK_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.network_zip_to_datacatalogue(self.source, self.target, self.job_strategy)
        elif self.job_strategy == JobStrategy.COHORT_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.cohort_zip_to_datacatalogue(self.source, self.target, self.job_strategy)
        elif self.job_strategy == JobStrategy.UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.umcg_cohort_zip_to_datacatalogue(self.source, self.target, self.job_strategy)
        elif self.job_strategy == JobStrategy.FILL_STAGING.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.fill_staging(self.source, self.target)
        elif self.job_strategy == JobStrategy.SHARED_STAGING.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.shared_staging(self.source, self.target)
        elif self.job_strategy == JobStrategy.UMCG_SHARED_ONTOLOGY_ZIP.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.umcg_shared_ontology_zip_to_datacatalogue(self.source, self.target, self.job_strategy)
        elif self.job_strategy == JobStrategy.ONTOLOGY_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.ontology_staging_zip_to_datacatalogue(self.source, self.target, self.job_strategy)
        elif self.job_strategy == JobStrategy.FILL_NETWORK.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.fill_network(self.source, self.target)
        else:
            log.error(f'Job Strategy not set, please use: ')
            log.error(JobStrategy.member_names())


class JobStrategy(Enum):
    COHORT_STAGING_TO_DATA_CATALOGUE_ZIP = auto()
    UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP = auto()
    NETWORK_STAGING_TO_DATA_CATALOGUE_ZIP = auto()
    UMCG_SHARED_ONTOLOGY_ZIP = auto()
    ONTOLOGY_STAGING_TO_DATA_CATALOGUE_ZIP = auto()
    FILL_STAGING = auto()
    SHARED_STAGING = auto()
    FILL_NETWORK = auto()

    @classmethod
    def member_names(cls):
        return list(map(lambda c: c.name, cls))
