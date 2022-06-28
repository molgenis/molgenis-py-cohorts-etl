import util

class Sync:
    def sync_fill_staging(self) -> None:
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

    def sync_shared_staging(self) -> None:
        """ Sync SOURCE (SharedStaging) with TARGET """
        tablesToSync = {
            'Institutions': None,
            'Contacts': None,
        }
        util.Util.download_filter_upload(self, tablesToSync)
    
    # def sync_cohort_staging_to_datacatalogue(self) -> None:
    #     # order of tables is important, value equals filter
    #     tablesToDelete = {
    #         'VariableMappings': 'mappings',
    #         'TableMappings': 'mappings',
    #         'SourceVariableValues': 'variables',
    #         'RepeatedSourceVariables': 'variables',
    #         'SourceVariables': 'variables',
    #         'SourceTables': 'variables',
    #         'SourceDataDictionaries': 'resource',
    #         'Documentation': 'resource',
    #         'Contributions': 'resource',
    #         'CollectionEvents': 'resource',
    #         'Subcohorts': 'resource',
    #         'Partners': 'resource',
    #         'Cohorts': 'pid',
    #     }

    #     tablesToSync = {
    #         'Cohorts': 'pid',
    #         'Partners': 'resource',
    #         'Contributions': 'resource',
    #         'Subcohorts': 'resource',
    #         'CollectionEvents': 'resource',
    #         'Documentation': 'resource',
    #         'SourceDataDictionaries': 'resource',
    #         'SourceTables': 'variables',
    #         'SourceVariables': 'variables',
    #         'SourceVariableValues': 'variables',
    #         'RepeatedSourceVariables': 'variables',
    #         'TableMappings': 'mappings',
    #         'VariableMappings': 'mappings',
            
    #     }

    #     util.Util.delete_cohort_from_data_catalogue(self, tablesToDelete)
    #     util.Util.download_upload(self, tablesToSync)
    
    # def sync_network_staging_to_datacatalogue(self) -> None:
    #     # order of tables is important, value equals filter
    #     tablesToDelete = {
    #         'TargetVariableValues': 'variables',
    #         'RepeatedTargetVariables': 'variables',
    #         'TargetVariables': 'variables',
    #         'TargetTables': 'variables',
    #         'TargetDataDictionaries': 'resource',
    #         'CollectionEvents': 'resource',
    #         'Subcohorts': 'resource',
    #     }

    #     tablesToSync = {
    #         #'Models': None,
    #         'TargetDataDictionaries': 'resource',
    #         'TargetTables': 'variables',
    #         'Subcohorts': 'resource',
    #         'RepeatedTargetVariables': 'variables',
    #         'CollectionEvents': 'resource',
    #         'TargetVariables': 'variables',
    #         'TargetVariableValues': 'variables',
    #     }

    #     #JobUtil.delete_network_from_data_catalogue(self, tablesToDelete)
    #     util.Util.download_upload(self, tablesToSync)
    
    def sync_datacatalogue_to_network_staging(self) -> None:
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

        util.Util.download_filter_upload(self, tablesToSync, network = True)

    # def sync_UMCG_cohort_to_UMCG_catalogue(self) -> None:
    #     """cohort rich metadata from UMCG cohort staging areas to catalogue."""
    #     tablesToDelete = {
    #         'Documentation': 'resource',
    #         'Contributions': 'resource',
    #         'CollectionEvents': 'resource',
    #         'Subcohorts': 'resource',
    #         'Partners': 'resource',
    #         'Cohorts': 'pid',
    #     }

    #     tablesToSync = {
    #         'Publications': None,
    #         'Cohorts': None,
    #         'Documentation': None,
    #         'Contributions': None,
    #         'Subcohorts': None,
    #         'CollectionEvents': None,
    #         'Partners': None,
    #     }
    #     util.Util.delete_cohort_from_data_catalogue(self, tablesToDelete)
    #     util.Util.download_upload(self, tablesToSync)
    def umcg_cohort_zip_to_datacatalogue(self) -> None:
        """Download UMCG cohort staging zip, delete cohort on datacatalogue and finaly upload transformed zip"""
        util.Util.download_target(self)
        
        # Database name needs to be identical to cohort PID
        tablesToDelete = {
            #'Publications': 'doi',
            'Documentation': 'resource',
            'Contributions': 'resource',
            'CollectionEvents': 'resource',
            'Subcohorts': 'resource',
            'Partners': 'resource',
            #'SubcohortCounts': 'resource',
            'Subcohorts': 'resource',
            #'Resources': 'pid',
            'Cohorts': 'pid',
        }

        tablesToSync = {
            'Publications': None,
            'Documentation': None,
            'Contributions': None,
            'CollectionEvents': None,
            'Subcohorts': None,
            'Partners': None,
            'SubcohortCounts': None,
            'Subcohorts': None,
            'Resources': None,
            'Cohorts': None,
        }
       
        util.Util.delete_cohort_from_data_catalogue(self, tablesToDelete)
        util.Util.download_zip_process(self, tablesToSync)

    def cohort_zip_to_datacatalogue(self) -> None:
        """Download cohort staging zip, delete cohort on datacatalogue and finaly upload transformed zip"""
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
    
    def umcg_shared_staging_zip_to_datacatalogue(self) -> None:
        """ UMCG data model uses SharedStaging Contacts and Instiutions directly (same server), no need to sync.
        Upload CoreVariables to CatalogueOntologies as a zip"""
        util.Util.download_target(self)

        # tablesToSync = {
        #     'Contacts': None,
        #     'Institutions': None,
        # }

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