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
            job_strategy: Enum) -> None:
        ''''''
        self.target_url = target_url
        self.target_email = target_email
        self.target_password = target_password
        self.target_database = target_database

        self.source_url = source_url
        self.source_email = source_email
        self.source_password = source_password
        self.source_database = source_database

        self.job_strategy = job_strategy

        # Set up Client for SOURCE
        self.source = client.Client(
            url=self.source_url,
            database=self.source_database,
            email=self.source_email,
            password=self.source_password
        )

        # Set up Client for TARGET
        self.target = client.Client(
            url=self.target_url,
            database=self.target_database,
            email=self.target_email,
            password=self.target_password
        )

        # Ensure database schemas exist, otherwise exit
        self.source.check_database_exists()
        self.target.check_database_exists()

    def run_strategy(self) -> None:

        if self.job_strategy == JobStrategy.NETWORK_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.network_zip_to_datacatalogue(self)
        elif self.job_strategy == JobStrategy.COHORT_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.cohort_zip_to_datacatalogue(self)
        elif self.job_strategy == JobStrategy.UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.umcg_cohort_zip_to_datacatalogue(self)
        elif self.job_strategy == JobStrategy.FILL_STAGING.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.fill_staging(self)
        elif self.job_strategy == JobStrategy.SHARED_STAGING.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.shared_staging(self)
        elif self.job_strategy == JobStrategy.UMCG_SHARED_ONTOLOGY_ZIP.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.umcg_shared_ontology_zip_to_datacatalogue(self)
        elif self.job_strategy == JobStrategy.ONTOLOGY_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.ontology_staging_zip_to_datacatalogue(self)
        elif self.job_strategy == JobStrategy.FILL_NETWORK.name:
            log.info(f'Run job strategy: {self.job_strategy}')
            sync.Sync.fill_network(self)
        else:
            log.error(f'Job Strategy not set, please use: ')
            log.error(JobStrategy._member_names_)


class JobStrategy(Enum):
    COHORT_STAGING_TO_DATA_CATALOGUE_ZIP = auto()
    UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP = auto()
    NETWORK_STAGING_TO_DATA_CATALOGUE_ZIP = auto()
    UMCG_SHARED_ONTOLOGY_ZIP = auto()
    ONTOLOGY_STAGING_TO_DATA_CATALOGUE_ZIP = auto()
    FILL_STAGING = auto()
    SHARED_STAGING = auto()
    FILL_NETWORK = auto()
