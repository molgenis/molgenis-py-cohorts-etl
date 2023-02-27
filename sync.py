import util


class Sync:
    def fill_staging(self) -> None:
        """ Sync SOURCE (catalogue) with TARGET (staging)
        """       
        # order of tables is important, value equals filter
        tablesToSync = {
            'Publications': None,
            'Cohorts': 'pid',
            'Documentation': 'resource',
            'Contributions': 'resource',
            'Subcohorts': 'resource',
            'CollectionEvents': 'resource',
            'Partners': 'resource',
            'SourceDataDictionaries': 'resource',
            'SourceTables': 'dataDictionary.resource',
            'SourceVariables': 'dataDictionary.resource',
            'RepeatedSourceVariables': 'dataDictionary.resource',
            'SourceVariableValues': 'dataDictionary.resource',
            'TableMappings': 'fromDataDictionary.resource',
            'VariableMappings': 'fromDataDictionary.resource',
        }
        util.Util.download_filter_upload(self, tablesToSync)

    def shared_staging(self) -> None:
        """ Sync SOURCE (SharedStaging) with TARGET """
        tablesToSync = {
            'Institutions': None,
            'Contacts': None,
        }
        util.Util.download_filter_upload(self, tablesToSync)

    def fill_network(self) -> None:
        # order of tables is important, value equals filter
        tablesToSync = {
            'Models': 'pid',
            'TargetDataDictionaries': 'resource',
            'TargetTables': 'dataDictionary.resource',
            'Subcohorts': 'resource',
            'CollectionEvents': 'resource',
            'TargetVariables': 'dataDictionary.resource',
            'TargetVariableValues': 'dataDictionary.resource',
            'RepeatedTargetVariables': 'dataDictionary.resource',
        }

        util.Util.download_filter_upload(self, tablesToSync, network=True)

    def umcg_cohort_zip_to_datacatalogue(self) -> None:
        """Download UMCGCohortStaging zip, delete cohort on DataCatalogue and finally upload transformed zip"""
        util.Util.download_target(self)
        
        # Database name needs to be identical to cohort PID
        tablesToDelete = {
            # 'Publications': 'doi',
            'Documentation': 'resource',
            'Contributions': 'resource',
            'CollectionEvents': 'resource',
            'Partners': 'resource',
            'SubcohortCounts': 'subcohort',
            'Subcohorts': 'resource',
            'Cohorts': 'pid',
        }

        tablesToSync = {
            'Publications': None,
            'Documentation': None,
            'Contributions': None,
            'CollectionEvents': None,
            # 'Subcohorts': None,
            'Partners': None,
            'SubcohortCounts': None,
            'Subcohorts': None,
            'Resources': None,
            'Cohorts': None,
        }
       
        util.Util.delete_cohort_from_data_catalogue(self, tablesToDelete)
        util.Util.download_zip_process(self, tablesToSync)

    def cohort_zip_to_datacatalogue(self) -> None:
        """Download cohort staging zip, delete cohort on DataCatalogue and finally upload transformed zip"""
        util.Util.download_target(self)
        
        # Database name needs to be identical to cohort PID
        tablesToDelete = {
            'VariableMappings': 'mappings',
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
            'Partners': 'resource',
            'Cohorts': 'pid',
        }

        tablesToSync = {
            'Publications': None,
            'VariableMappings': None,
            'TableMappings': None,
            'SourceVariableValues': None,
            'RepeatedSourceVariables': None,
            'SourceVariables': None,
            'SourceTables': None,
            'SourceDataDictionaries': None,
            'Documentation': None,
            'Contributions': None,
            'CollectionEvents': None,
            'Subcohorts': None,
            'Partners': None,
            'Cohorts': None,
        }
       
        util.Util.delete_cohort_from_data_catalogue(self, tablesToDelete)
        util.Util.download_zip_process(self, tablesToSync)
    
    def network_zip_to_datacatalogue(self) -> None:
        tablesToSync = {
            'AllTargetVariables': None,
            'CollectionEvents': None,
            'Models': None,
            'Networks': None,
            'RepeatedTargetVariables': None,
            'Resources': None,
            'Subcohorts': None,
            'TargetDataDictionaries': None,
            'TargetTables': None,
            'TargetVariables': None,
            'TargetVariableValues': None,

        }
        util.Util.download_zip_process(self, tablesToSync)
    
    def umcg_shared_ontology_zip_to_datacatalogue(self) -> None:
        """ UMCG data model uses SharedStaging Contacts and Institutions directly (same server), no need to sync.
        Upload CoreVariables to CatalogueOntologies as a zip"""
        util.Util.download_target(self)

        ontologiesToSync = {
            'CoreVariables': None
        }
        util.Util.download_zip_process(self, ontologiesToSync)
    
    def ontology_staging_zip_to_datacatalogue(self) -> None:
        """ Upload SOURCE CatalogueOntologies to TARGET CatalogueOntologies as a zip"""
        util.Util.download_target(self)

        ontologiesToSync = {
            'AgeGroups': None,
            'AreasOfInformation': None,
            'CareSettings': None,
            'CohortDesigns': None,
            'CollectionTypes': None,
            'ContributionTypes': None,
            'CoreVariables': None,
            'Countries': None,
            'DAPsAccessCompleteness': None,
            'DAPsAccessLevels': None,
            'DAPsAccessPermissions': None,
            'DAPsReasonsForAccess': None,
            'DataAccessConditions': None,
            'DataCategories': None,
            'DataUseConditions': None,
            'DatabankFamilies': None,
            'DatasourceTypes': None,
            'Diseases': None,
            'DocumentTypes': None,
            'Formats': None,
            'InformedConsents': None,
            'InstitutionRoles': None,
            'InstitutionTypes': None,
            'Keywords': None,
            'Languages': None,
            'LinkageStrategies': None,
            'Months': None,
            'ObservationTargets': None,
            'PartnerRoles': None,
            'PopulationEntryCauses': None,
            'PopulationExitCauses': None,
            'PopulationSubsets': None,
            'Regions': None,
            'ReleaseTypes': None,
            'ResourceTypes': None,
            'SampleCategories': None,
            'StandardizedTools': None,
            'Status': None,
            'StatusDetails': None,
            'StudyTypes': None,
            'Titles': None,
            'Units': None,
            'Vocabularies': None,
            'Years': None,
        }
        util.Util.download_zip_process(self, ontologiesToSync)
