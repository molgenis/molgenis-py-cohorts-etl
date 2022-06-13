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
        # order of tables is important, value equals filter
        tablesToSync = {
            'Institutions': None,
            'Contacts': None,
        }
        util.Util.download_filter_upload(self, tablesToSync)
    
    def sync_cohort_staging_to_datacatalogue(self) -> None:
        # order of tables is important, value equals filter
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
            'Cohorts': 'pid',
            'Partners': 'resource',
            'Contributions': 'resource',
            'Subcohorts': 'resource',
            'CollectionEvents': 'resource',
            'Documentation': 'resource',
            'SourceDataDictionaries': 'resource',
            'SourceTables': 'variables',
            'SourceVariables': 'variables',
            'SourceVariableValues': 'variables',
            'RepeatedSourceVariables': 'variables',
            'TableMappings': 'mappings',
            'VariableMappings': 'mappings',
            
        }

        util.Util.delete_cohort_from_data_catalogue(self, tablesToDelete)
        util.Util.download_upload(self, tablesToSync)
    
    def sync_network_staging_to_datacatalogue(self) -> None:
        # order of tables is important, value equals filter
        tablesToDelete = {
            'TargetVariableValues': 'variables',
            'RepeatedTargetVariables': 'variables',
            'TargetVariables': 'variables',
            'TargetTables': 'variables',
            'TargetDataDictionaries': 'resource',
            'CollectionEvents': 'resource',
            'Subcohorts': 'resource',
        }

        tablesToSync = {
            #'Models': None,
            'TargetDataDictionaries': 'resource',
            'TargetTables': 'variables',
            'Subcohorts': 'resource',
            'RepeatedTargetVariables': 'variables',
            'CollectionEvents': 'resource',
            'TargetVariables': 'variables',
            'TargetVariableValues': 'variables',
        }

        #JobUtil.delete_network_from_data_catalogue(self, tablesToDelete)
        util.Util.download_upload(self, tablesToSync)
    
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

    def sync_UMCG_cohort_to_UMCG_catalogue(self) -> None:
        """cohort rich metadata from UMCG cohort staging areas to catalogue."""
        tablesToDelete = {
            'Documentation': 'resource',
            'Contributions': 'resource',
            'CollectionEvents': 'resource',
            'Subcohorts': 'resource',
            'Partners': 'resource',
            'Cohorts': 'pid',
        }

        tablesToSync = {
            'Publications': None,
            'Cohorts': None,
            'Documentation': None,
            'Contributions': None,
            'Subcohorts': None,
            'CollectionEvents': None,
            'Partners': None,
        }
        util.Util.delete_cohort_from_data_catalogue(self, tablesToDelete)
        util.Util.download_upload(self, tablesToSync)

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
       

        util.Util.delete_cohort_from_data_catalogue(self, tablesToDelete)
        util.Util.download_cohort_zip_process(self, tablesToSync)