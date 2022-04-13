from job import Job
import logging

log = logging.getLogger(__name__)


class JobUMCGCohorts(Job):
    """
    Class to copy cohort rich metadata from UMCG cohort staging areas to catalogue.
    """

    def __init__(self, url, email, password, catalogueDB, sourceDB):
        super(Job, self).__init__(url, email, password, catalogueDB, sourceDB)

        self.cohortPid = self.fetchCohortPid(self.staging, self.sourceDB)

        if self.cohortPid is None:
            log.info('Skip sync for: ' + self.sourceDB)
        else:
            self.syncCohort()

    def syncCohort(self):
        """Sync staging with catalogue"""
        tablesToSync = {'Documentation': 'cohorts',
                        'Contributions': 'cohorts',
                        'CollectionEvents': 'cohorts',
                        'Subcohorts': 'cohorts',
                        'Partners': 'cohorts'}

        # 1) Delete from catalogue
        for tableName in tablesToSync:
            tableType = tablesToSync[tableName]
            self.catalogueClient.deleteTableContentsByPid(tableName, tableType, self.cohortPid)
        self.catalogue.delete('Cohorts', [{'pid': self.cohortPid}])

        # # 2) Download from staging
        newCohorts = self.download('Cohorts')
        newDocumentation = self.download('Documentation')
        newContributions = self.download('Contributions')
        newCollectionEvents = self.download('CollectionEvents')
        newSubcohorts = self.download('Subcohorts')
        newPartners = self.download('Partners')
        newPublications = self.download('Publications')

        # # 3) Add/Upload to catalog
        self.uploadIfSet('Publications', newPublications)
        self.uploadIfSet('Cohorts', newCohorts)
        self.uploadIfSet('Documentation', newDocumentation)
        self.uploadIfSet('Contributions', newContributions)
        self.uploadIfSet('Subcohorts', newSubcohorts)
        self.uploadIfSet('CollectionEvents', newCollectionEvents)
        self.uploadIfSet('Partners', newPartners)
