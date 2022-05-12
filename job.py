import pandas as pd
from io import BytesIO, StringIO
from pathlib import Path

import client
import logging

log = logging.getLogger(__name__)

class Job:
    def __init__(
        self,
        target_url: str,
        target_email: str,
        target_password: str,
        target_database: str,
        source_url: str,
        source_email: str,
        source_password: str,
        source_database: str,
        job_strategy: str) -> None:
        ''''''
        self.target_url = target_url
        self.target_email = target_email
        self.target_password = target_password
        self.target_database = target_database
        self.source_url = source_url
        self.source_email = source_email
        self.source_password = source_password
        self.source_database = source_database
        self.job_strategy = job_strategy

        # set up Client for SOURCE
        self.source = client.Client(
            url = self.source_url,
            database = self.source_database,
            email = self.source_email,
            password = self.source_password
        )

        # set up Client for TARGET
        self.target = client.Client(
            url = self.target_url,
            database = self.target_database,
            email = self.target_email,
            password = self.target_password
        )
        
        JobStrategy.strategy(self)

class JobStrategy:
    def strategy(self) -> None:
        if self.job_strategy == 'FillStaging': # FillStagingCohorts
            log.info('Run job strategy: ' + self.job_strategy)
            JobSync.sync_fill_staging(self)
        
        elif self.job_strategy == 'SharedStaging':
            log.info('Run job strategy: ' + self.job_strategy)
            JobSync.sync_shared_staging(self)
        
        elif self.job_strategy == 'CohortStagingToDataCatalogue': 
            log.info('Run job strategy: ' + self.job_strategy)
            if JobUtil.get_source_cohort_pid(self):
                JobSync.sync_cohort_staging_to_datacatalogue(self)
        
        elif self.job_strategy == 'NetworkStagingToDataCatalogue':
            log.info('Run job strategy: ' + self.job_strategy)
            if JobUtil.get_source_model_pid(self):
                JobSync.sync_network_staging_to_datacatalogue(self)
            #Job.sync_network_staging_to_datacatalogue(self)
        
        elif self.job_strategy == 'DataCatalogueToNetworkStaging': # FillStagingNetwork
            log.info('Run job strategy: ' + self.job_strategy)
            JobSync.sync_datacatalogue_to_network_staging(self)

        elif self.job_strategy == 'UMCGCohorts':
            log.info('Run job strategy: ' + self.job_strategy)
            JobSync.sync_UMCG_cohort_to_UMCG_catalogue(self)

        # TODO onotolgies,
        # TODO files (eerst files dan molgenis)

        else:
            log.error('Job Strategy not set, please use: FillStaging, SharedStaging, CohortStagingToDataCatalogue, NetworkStagingToDataCatalogue, DataCatalogueToNetworkStaging, UMCGCohorts')

class JobSync:
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
        JobUtil.download_filter_upload(self, tablesToSync)

    def sync_shared_staging(self) -> None:
        """ Sync SOURCE (SharedStaging) with TARGET """
        # order of tables is important, value equals filter
        tablesToSync = {
            'Institutions': None,
            'Contacts': None,
        }
        JobUtil.download_filter_upload(self, tablesToSync)
    
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

        JobUtil.delete_cohort_from_data_catalogue(self, tablesToDelete)
        JobUtil.download_upload(self, tablesToSync)
    
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
        JobUtil.download_upload(self, tablesToSync)
    
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

        JobUtil.download_filter_upload(self, tablesToSync, network = True)

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
        JobUtil.delete_cohort_from_data_catalogue(self, tablesToDelete)
        JobUtil.download_upload(self, tablesToSync)

class JobUtil:
    def download_source_data(self, table: str) -> bytes:
        """ Download catalogue data or return None in case of zero rows """
        result = client.Client.query(self.source, 'query Count{' + table + '_agg { count }}')
        if result[table + '_agg']['count'] > 0:
            return client.Client.downLoadCSV(self.source, table)
        return None
    
    def download_filter_upload(self, tablesToSync: dict, network: bool = False) -> None:
        """ Download SOURCE csv, filter with pandas and upload csv to TARGET"""
        if network:
            databases = [self.target_database, self.target_database + '_CDM']
        else:
            databases = [self.target_database]

        #print(databases)
        for table in tablesToSync:
            filter = tablesToSync[table]
            data = JobUtil.download_source_data(self, table)
            
            if data != None:
                df = pd.read_csv(BytesIO(data), dtype='str', na_filter=False) # dtype set to string otherwise numbers will be converted to 2015 -> 2015.0

                stream = StringIO()
                
                for database in databases:
                    if filter != None:
                        df_filter = df[filter] == database
                        if not df[df_filter].empty:
                            df[df_filter].to_csv(stream, index=False)
                    else:
                        df.to_csv(stream, index=False)

                    if stream.getvalue():
                        uploadResponse = client.Client.uploadCSV(
                            self.target, 
                            table, 
                            stream.getvalue().encode('utf-8')
                        )
                log.info(str(table) + ' ' + str(uploadResponse))
    
    def download_upload(self, tablesToSync: dict) -> None:
        """ Download SOURCE csv and upload csv to TARGET"""
        for table in tablesToSync:
            data = JobUtil.download_source_data(self, table)
            
            if data != None:
                df = pd.read_csv(BytesIO(data), dtype='str', na_filter=False) # dtype set to string otherwise numbers will be converted to 2015 -> 2015.0

                stream = StringIO()
                              
                df.to_csv(stream, index=False)

                uploadResponse = client.Client.uploadCSV(
                    self.target, 
                    table, 
                    stream.getvalue().encode('utf-8')
                )
                log.info(str(table) + ' ' + str(uploadResponse))
    
    def get_source_cohort_pid(self) -> str:
        """ get PID of SOURCE cohort, expects to get one PID, return pid. """
        try:
            result = self.source.query(Path('./graphql-queries/' + 'Cohorts.gql').read_text())
            if "Cohorts" in result:
                if len(result['Cohorts']) != 1:
                    log.warning('Expected a single cohort in staging area "' + self.source_database + '" but found ' + str(len(result['Cohorts'])))
                    return None
            else:
                log.warning('Expected a single cohort in staging area "' + self.source_database + '" but found none')
                return None

            return result['Cohorts'][0]['pid']
        except KeyError:
            log.error('Staging area "' + self.source_database + ' does not contain a table "Cohorts".')
            return None
    
    def get_source_model_pid(self) -> str:
        """ get PID of SOURCE network, expects to get one PID, return pid.
        Fetch first model and return pid or else fail.
        Not all staging areas (SharedStaging) contain a table 'Models', therefore a try/except is used
        here.
        """
        try:
            result = self.source.query(Path('./graphql-queries/' + 'Models.gql').read_text())
            if "Models" in result:
                if len(result['Models']) != 1:
                    log.warning('Expected a single model in staging area "' + self.source_database + '" but found ' + str(len(result['Models'])) + ': ' + str(result['Models']))
                    return None
            else:
                log.warning('Expected a single model in staging area "' + self.source_database + '" but found none')
                return None

            return result['Models'][0]['pid']
        except KeyError:
            log.info('Staging area "' + result + '" does not contain a table "Models".')
            return None

    def delete_cohort_from_data_catalogue(self, tablesToSync: dict) -> None:
        """ Delete SOURCE Cohort data from TARGET data catalogue before upload """
        rowsToDelete = []

        for tableName in tablesToSync:
            tableType = tablesToSync[tableName]

            query = Path('./graphql-queries/' + tableName + '.gql').read_text()
        
            if tableType == 'resource':
                variables = {"filter": {"resource": {"equals": [{"pid": JobUtil.get_source_cohort_pid(self)}]}}}
            elif tableType == 'mappings':
                variables = {"filter": {"fromDataDictionary": {"resource": {"equals": [{"pid": JobUtil.get_source_cohort_pid(self)}]}}}}
            elif tableType == 'variables':
                variables = {"filter": {"dataDictionary": {"resource": {"equals": [{"pid": JobUtil.get_source_cohort_pid(self)}]}}}}
            elif tableType == 'pid':
                variables = {"filter": {"equals": [{"pid": JobUtil.get_source_cohort_pid(self)}]}}
                
            result = self.target.query(query, variables)
        
            if tableName in result:
                client.Client.delete(self.target, tableName, result[tableName])

    def delete_network_from_data_catalogue(self, tablesToSync: dict) -> None:
        """ Delete SOURCE Network data from TARGET data catalogue before upload """
        rowsToDelete = []

        for tableName in tablesToSync:
            tableType = tablesToSync[tableName]

            query = Path('./graphql-queries/' + tableName + '.gql').read_text()
        
            if tableType == 'resource':
                variables = {"filter": {"resource": {"equals": [{"pid": JobUtil.get_source_model_pid(self)}]}}}
            elif tableType == 'mappings':
                variables = {"filter": {"fromDataDictionary": {"resource": {"equals": [{"pid": JobUtil.get_source_model_pid(self)}]}}}}
            elif tableType == 'variables':
                variables = {"filter": {"dataDictionary": {"resource": {"equals": [{"pid": JobUtil.get_source_model_pid(self)}]}}}}

            result = self.target.query(query, variables)
        
            if tableName in result:
                client.Client.delete(self.target, tableName, result[tableName])