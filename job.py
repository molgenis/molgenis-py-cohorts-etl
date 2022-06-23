import client
import strategy
import logging

from enum import Enum, auto

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

        # set up Client for SOURCE
        self.source = client.Client(
            url = self.source_url,
            database = self.source_database,
            email = self.source_email,
            password = self.source_password
        )

        # set up Client for TARGET
        self.target = client.Client(
            url = self.target_url,
            database = self.target_database,
            email = self.target_email,
            password = self.target_password
        )

        # Make sure database schemas exists otherwise exit
        client.Client.database_exists(self.source)
        client.Client.database_exists(self.target)

        strategy.Strategy.run_strategy(self)

class JobStrategy(Enum):
    COHORT_STAGING_TO_DATA_CATALOGUE_ZIP = auto()
    UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP = auto()
    NETWORK_STAGING_TO_DATA_CATALOGUE_ZIP = auto()
    FILL_STAGING = auto()
    SHARED_STAGING = auto()