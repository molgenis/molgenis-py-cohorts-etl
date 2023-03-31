import logging
import sys
from enum import Enum, auto

from src.client import Client
from sync import Sync

log = logging.getLogger(__name__)


class Job:
    """A Job object consists of a Client object for the source database, a Client object for
    the target database and the Job strategy.
    """
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

        # Set up Client for SOURCE
        self.source = Client(
            url=source_url,
            database=source_database,
            email=source_email,
            password=source_password
        )

        # Set up Client for TARGET
        self.target = Client(
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
        """Checks if the job strategy is valid and executes the corresponding Sync function."""

        if self.job_strategy not in JobStrategy.member_names():
            log.error(f'Job Strategy not set, please use: \n'
                      f'{JobStrategy.member_names()}')
            sys.exit()

        log.info(f'Run job strategy: {self.job_strategy}.')
        match self.job_strategy:
            case JobStrategy.NETWORK_STAGING_TO_DATA_CATALOGUE_ZIP.name:
                Sync.network_zip_to_datacatalogue(self.source, self.target, self.job_strategy)
            case JobStrategy.COHORT_STAGING_TO_DATA_CATALOGUE_ZIP.name:
                Sync.cohort_zip_to_datacatalogue(self.source, self.target, self.job_strategy)
            case JobStrategy.UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP.name:
                Sync.umcg_cohort_zip_to_datacatalogue(self.source, self.target, self.job_strategy)
            case JobStrategy.FILL_STAGING.name:
                Sync.fill_staging(self.source, self.target)
            case JobStrategy.SHARED_STAGING.name:
                Sync.shared_staging(self.source, self.target)
            case JobStrategy.UMCG_SHARED_ONTOLOGY_ZIP.name:
                Sync.umcg_shared_ontology_zip_to_datacatalogue(self.source, self.target, self.job_strategy)
            case JobStrategy.ONTOLOGY_STAGING_TO_DATA_CATALOGUE_ZIP.name:
                Sync.ontology_staging_zip_to_datacatalogue(self.source, self.target, self.job_strategy)
            case JobStrategy.FILL_NETWORK.name:
                Sync.fill_network(self.source, self.target)


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
