import job
#import sync
#import util
import migration
import logging

log = logging.getLogger(__name__)

class Strategy:
    def run_strategy(self) -> None:

        if self.job_strategy == job.JobStrategy.UMCG_CATALOGUE.name:
            log.info('Run job strategy: ' + self.job_strategy)
            #sync.Sync.network_zip_to_datacatalogue(self)
            migration.Migration.migrate_umcg_catalogue(self)
        
        else:
            log.error(f'Job Strategy not set, please use: ')
            log.error(job.JobStrategy._member_names_)