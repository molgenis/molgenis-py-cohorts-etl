from job import Job


class JobUMCGCohorts(Job):
    """
    Class to copy cohort rich metadata from UMCG cohort staging areas to catalogue.
    """

    def __init__(self, url, email, password, catalogueDB, sourceDB):
        super(Job, self).__init__(url, email, password, catalogueDB, sourceDB)

        self.cohortPid = self.fetchCohortPid(self.staging, self.sourceDB)
        self.tablesToSync = {'Documentation': 'cohorts',
                             'Contributions': 'cohorts',
                             'CollectionEvents': 'cohorts',
                             'Subcohorts': 'cohorts',
                             'Partners': 'cohorts'}

        # Fetch cohort pid
        self.cohortPid = self.fetchCohortPid(self.staging, self.sourceDB)

    def sync(self):
        """Sync staging with catalogue"""
        if self.cohortPid is None:
            self.log.info('Skip sync for: ' + self.sourceDB)

        # 1) Delete from catalogue
        for tableName in self.tablesToSync:
            tableType = self.tablesToSync[tableName]
            self.catalogueClient.deleteTableContentsByPid(tableName, tableType, self.cohortPid)

        self.catalogue.delete('Cohorts', [{'pid': self.cohortPid}])

        # # 2) Download from staging
        newCohorts = self.download('Cohorts')
        newDocumentation = self.download('Documentation')
        newContacts = self.download('Contacts')  # TODO: Contacts should be in shared staging area
        newContributions = self.download('Contributions')
        newCollectionEvents = self.download('CollectionEvents')
        newSubcohorts = self.download('Subcohorts')
        newPartners = self.download('Partners')
        newPublications = self.download('Publications')

        # # 3) Add/Upload to catalog
        self.uploadIfSet('Publications', newPublications)
        self.uploadIfSet('Cohorts', newCohorts)
        self.uploadIfSet('Documentation', newDocumentation)
        self.uploadIfSet('Contacts', newContacts)  # TODO: Contacts should be in shared staging area
        self.uploadIfSet('Contributions', newContributions)
        self.uploadIfSet('Subcohorts', newSubcohorts)
        self.uploadIfSet('CollectionEvents', newCollectionEvents)
        self.uploadIfSet('Partners', newPartners)
