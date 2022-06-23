import job
import sync
#import util
import logging

log = logging.getLogger(__name__)

class Strategy:
    def run_strategy(self) -> None:
        # if self.job_strategy == 'FillStaging': # FillStagingCohorts
        #     log.info('Run job strategy: ' + self.job_strategy)
        #     sync.Sync.sync_fill_staging(self)
        
        # elif self.job_strategy == 'SharedStaging':
        #     log.info('Run job strategy: ' + self.job_strategy)
        #     sync.Sync.sync_shared_staging(self)
        
        # elif self.job_strategy == 'CohortStagingToDataCatalogue': 
        #     log.info('Run job strategy: ' + self.job_strategy)
        #     if util.Util.get_source_cohort_pid(self):
        #         sync.Sync.sync_cohort_staging_to_datacatalogue(self)
        
        # elif self.job_strategy == 'NetworkStagingToDataCatalogue':
        #     log.info('Run job strategy: ' + self.job_strategy)
        #     if util.Util.get_source_model_pid(self):
        #         sync.Sync.sync_network_staging_to_datacatalogue(self)
        #     #Job.sync_network_staging_to_datacatalogue(self)
        
        # elif self.job_strategy == 'DataCatalogueToNetworkStaging': # FillStagingNetwork
        #     log.info('Run job strategy: ' + self.job_strategy)
        #     sync.Sync.sync_datacatalogue_to_network_staging(self)

        # elif self.job_strategy == 'UMCGCohorts':
        #     log.info('Run job strategy: ' + self.job_strategy)
        #     sync.Sync.sync_UMCG_cohort_to_UMCG_catalogue(self)

        # # TODO onotolgies
        # elif self.job_strategy == 'CohortStagingToDataCatalogue_Zip':
        #     log.info('Run job strategy: ' + self.job_strategy)
        #     sync.Sync.cohort_zip_to_datacatalogue(self)
        # elif self.job_strategy == 'NetworkStagingToDataCatalogue_Zip':
        #     log.info('Run job strategy: ' + self.job_strategy)
        #     sync.Sync.network_zip_to_datacatalogue(self)
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
            sync.Sync.sync_fill_staging(self)
        elif self.job_strategy == job.JobStrategy.SHARED_STAGING.name:
            log.info('Run job strategy: ' + self.job_strategy)
            sync.Sync.sync_shared_staging(self)
        else:
            log.error(f'Job Strategy not set, please use: ')
            log.error(job.JobStrategy._member_names_)