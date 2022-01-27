from job import Job


class JobDataCatalogueCohorts(Job):
    """
    Class to copy cohort rich metadata and variable metadata from data catalogue
    cohort staging areas to catalogue.
    """

    def __init__(self, url, email, password, catalogueDB, sourceDB):
        super().__init__(url, email, password, catalogueDB, sourceDB)

        self.cohortPid = self.fetchCohortPid(self.staging, self.sourceDB)
        self.tablesToSync = {'VariableMappings': 'mappings',
                             'TableMappings': 'mappings',
                             'SourceVariableValues': 'variables',
                             'RepeatedSourceVariables': 'variables',
                             'SourceVariables': 'variables',
                             'SourceTables': 'variables',
                             'SourceDataDictionaries': 'resource',
                             'Documentation': 'resource',
                             'Contributions': 'resource',
                             'CollectionEvents': 'resource',
                             'Subcohorts': 'resource',
                             'Partners': 'resource'}

    def sync(self):
        """Sync staging with catalogue"""
        if self.cohortPid is None:
            self.log.info('Skip sync for: ' + self.sourceDB)

        # 1) Delete from catalogue
        for tableName in self.tablesToSync:
            tableType = self.tablesToSync[tableName]
            self.catalogueClient.deleteTableContentsByPid(tableName, tableType, self.cohortPid)
        self.catalogue.delete('Cohorts', [{'pid': self.cohortPid}])

        # 2) Download from staging
        newVariableMappings = self.download('VariableMappings')
        newTableMappings = self.download('TableMappings')
        newSourceVariableValues = self.download('SourceVariableValues')
        newRepeatedSourceVariables = self.download('RepeatedSourceVariables')
        newSourceVariables = self.download('SourceVariables')
        newSourceTables = self.download('SourceTables')
        newSourceDataDictionaries = self.download('SourceDataDictionaries')
        newCohorts = self.download('Cohorts')
        newDocumentation = self.download('Documentation')
        newContacts = self.download('Contacts')  # TODO: Contacts should be in shared staging area
        newContributions = self.download('Contributions')
        newCollectionEvents = self.download('CollectionEvents')
        newSubcohorts = self.download('Subcohorts')
        newPartners = self.download('Partners')
        newPublications = self.download('Publications')

        # # 3) Add/Upload to catalogue
        self.uploadIfSet('Publications', newPublications)
        self.uploadIfSet('Cohorts', newCohorts)
        self.uploadIfSet('Documentation', newDocumentation)
        self.uploadIfSet('Contacts', newContacts)  # TODO: Contacts should be in shared staging area
        self.uploadIfSet('Contributions', newContributions)
        self.uploadIfSet('Subcohorts', newSubcohorts)
        self.uploadIfSet('CollectionEvents', newCollectionEvents)
        self.uploadIfSet('Partners', newPartners) # TODO: Institutions should be in shared staging area
        self.uploadIfSet('SourceDataDictionaries', newSourceDataDictionaries)
        self.uploadIfSet('SourceTables', newSourceTables)
        self.uploadIfSet('SourceVariables', newSourceVariables)
        self.uploadIfSet('RepeatedSourceVariables', newRepeatedSourceVariables)
        self.uploadIfSet('SourceVariableValues', newSourceVariableValues)
        self.uploadIfSet('TableMappings', newTableMappings)
        self.uploadIfSet('VariableMappings', newVariableMappings)


class JobDataCatalogueNetworks(Job):
    """
    Class to copy network model variable metadata from data catalogue network staging areas to
    catalogue.
    """

    def __init__(self, url, email, password, catalogueDB, sourceDB):
        super().__init__(url, email, password, catalogueDB, sourceDB)

        self.modelPid = self.fetchModelPid(self.staging, self.sourceDB)
        self.tablesToSync = {'TargetVariableValues': 'variables',
                             'RepeatedTargetVariables': 'variables',
                             'TargetVariables': 'variables',
                             'TargetTables': 'variables',
                             'TargetDataDictionaries': 'resource',
                             'CollectionEvents': 'resource',
                             'Subcohorts': 'resource'}

    def sync(self):
        """Sync staging with catalogue"""
        if self.modelPid is None:
            self.log.info('Skip sync for: ' + self.sourceDB)

        # 1) Delete from catalogue
        for tableName in self.tablesToSync:
            tableType = self.tablesToSync[tableName]
            self.catalogueClient.deleteTableContentsByPid(tableName, tableType, self.modelPid)

        # 2) Download from staging
        newTargetVariableValues = self.download('TargetVariableValues')
        newRepeatedTargetVariables = self.download('RepeatedTargetVariables')
        newTargetVariables = self.download('TargetVariables')
        newTargetTables = self.download('TargetTables')
        newTargetDataDictionaries = self.download('TargetDataDictionaries')
        newModels = self.download('Models')
        newCollectionEvents = self.download('CollectionEvents')
        newSubcohorts = self.download('Subcohorts')

        # # 3) Add/Upload to catalogue
        self.uploadIfSet('Models', newModels)
        self.uploadIfSet('Subcohorts', newSubcohorts)
        self.uploadIfSet('CollectionEvents', newCollectionEvents)
        self.uploadIfSet('TargetDataDictionaries', newTargetDataDictionaries)
        self.uploadIfSet('TargetTables', newTargetTables)
        self.uploadIfSet('TargetVariables', newTargetVariables)
        self.uploadIfSet('RepeatedTargetVariables', newRepeatedTargetVariables)
        self.uploadIfSet('TargetVariableValues', newTargetVariableValues)
