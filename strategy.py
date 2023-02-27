import logging

import job
import sync

log = logging.getLogger(__name__)


class Strategy:
    def run_strategy(self) -> None:

        if self.job_strategy == job.JobStrategy.NETWORK_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            log.info('Run job strategy: ' + self.job_strategy)
            sync.Sync.network_zip_to_datacatalogue(self)
        elif self.job_strategy == job.JobStrategy.COHORT_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            log.info('Run job strategy: ' + self.job_strategy)
            sync.Sync.cohort_zip_to_datacatalogue(self)
        elif self.job_strategy == job.JobStrategy.UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            log.info('Run job strategy: ' + self.job_strategy)
            sync.Sync.umcg_cohort_zip_to_datacatalogue(self)
        elif self.job_strategy == job.JobStrategy.FILL_STAGING.name:
            log.info('Run job strategy: ' + self.job_strategy)
            sync.Sync.fill_staging(self)
        elif self.job_strategy == job.JobStrategy.SHARED_STAGING.name:
            log.info('Run job strategy: ' + self.job_strategy)
            sync.Sync.shared_staging(self)
        elif self.job_strategy == job.JobStrategy.UMCG_SHARED_ONTOLOGY_ZIP.name:
            log.info('Run job strategy: ' + self.job_strategy)
            sync.Sync.umcg_shared_ontology_zip_to_datacatalogue(self)
        elif self.job_strategy == job.JobStrategy.ONTOLOGY_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            log.info('Run job strategy: ' + self.job_strategy)
            sync.Sync.ontology_staging_zip_to_datacatalogue(self)
        elif self.job_strategy == job.JobStrategy.FILL_NETWORK.name:
            log.info('Run job strategy: ' + self.job_strategy)
            sync.Sync.fill_network(self)
        else:
            log.error(f'Job Strategy not set, please use: ')
            log.error(job.JobStrategy._member_names_)
