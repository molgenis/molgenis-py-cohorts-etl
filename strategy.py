import sync
import util
import logging

log = logging.getLogger(__name__)

class Strategy:
    def strategy(self) -> None:
        if self.job_strategy == 'FillStaging': # FillStagingCohorts
            log.info('Run job strategy: ' + self.job_strategy)
            sync.Sync.sync_fill_staging(self)
        
        elif self.job_strategy == 'SharedStaging':
            log.info('Run job strategy: ' + self.job_strategy)
            sync.Sync.sync_shared_staging(self)
        
        elif self.job_strategy == 'CohortStagingToDataCatalogue': 
            log.info('Run job strategy: ' + self.job_strategy)
            if util.Util.get_source_cohort_pid(self):
                sync.Sync.sync_cohort_staging_to_datacatalogue(self)
        
        elif self.job_strategy == 'NetworkStagingToDataCatalogue':
            log.info('Run job strategy: ' + self.job_strategy)
            if util.Util.get_source_model_pid(self):
                sync.Sync.sync_network_staging_to_datacatalogue(self)
            #Job.sync_network_staging_to_datacatalogue(self)
        
        elif self.job_strategy == 'DataCatalogueToNetworkStaging': # FillStagingNetwork
            log.info('Run job strategy: ' + self.job_strategy)
            sync.Sync.sync_datacatalogue_to_network_staging(self)

        elif self.job_strategy == 'UMCGCohorts':
            log.info('Run job strategy: ' + self.job_strategy)
            sync.Sync.sync_UMCG_cohort_to_UMCG_catalogue(self)

        # TODO onotolgies
        elif self.job_strategy == 'CohortStagingToDataCatalogue_Zip':
            log.info('Run job strategy: ' + self.job_strategy)
            sync.Sync.cohort_zip_to_datacatalogue(self)

        else:
            log.error('Job Strategy not set, please use: FillStaging, SharedStaging, CohortStagingToDataCatalogue, NetworkStagingToDataCatalogue, DataCatalogueToNetworkStaging, UMCGCohorts')