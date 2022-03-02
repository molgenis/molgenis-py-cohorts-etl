import pandas as pd
from io import BytesIO, StringIO
from pathlib import Path

import client
#from catalogueClient import CatalogueClient
#from pathlib import Path
import logging

log = logging.getLogger(__name__)
# TODO: add copying of catalogue from data-catalogue-staging to catalogue on data-catalogue to JobDataCatalogue
# TODO: add more insightful error messages

# class Job:
#     """
    
#     """

#     def __init__(self, url, email, password, catalogueDB, sourceDB):
#         self.url = url
#         self.email = email
#         self.password = password
#         self.catalogueDB = catalogueDB
#         self.sourceDB = sourceDB

#         # Client serves as a general exm2 api client
#         self.staging = Client(url=self.url, database=sourceDB, email=self.email, password=self.password)
#         self.catalogue = Client(url=self.url, database=self.catalogueDB, email=self.email, password=self.password)
#         # CatalogueClient serves client specific for use with the catalog model
#         self.catalogueClient = CatalogueClient(self.staging, self.catalogue)

#     def uploadIfSet(self, table, data):
#         """ Upload staging data to catalogue if staging table contains data (else skip) """
#         if data is None:
#             return
#         uploadResp = self.catalogue.uploadCSV(table, data)
#         log.info('upload ' + table + ' ; ' + str(uploadResp))

#     def download(self, table):
#         """ Download staging data or return None in case of zero rows """
#         countResp = self.staging.query('query Count{' + table + '_agg { count }}')
#         if countResp[table + '_agg']['count'] > 0:
#             return self.staging.downLoadCSV(table)
#         else:
#             return None

#     def fetchCohortPid(self, staging, schemaName):
#         """ Fetch first cohort and return pid or else fail
#         Not all staging areas (network staging areas, SharedStaging) contain a table 'Cohorts',
#         therefore a try/except is used here.
#         """
#         try:
#             cohortsResp = staging.query(Path('./graphql-queries/' + 'Cohorts.gql').read_text())
#             if "Cohorts" in cohortsResp:
#                 if len(cohortsResp['Cohorts']) != 1:
#                     log.warning('Expected a single cohort in staging area "' + schemaName + '" but found ' + str(
#                         len(cohortsResp['Cohorts'])))
#                     return None
#             else:
#                 log.warning('Expected a single cohort in staging area "' + schemaName + '" but found none')
#                 return None

#             return cohortsResp['Cohorts'][0]['pid']
#         except KeyError:
#             log.info('Staging area "' + schemaName + ' does not contain a table "Cohorts".')
#             return None

#     def fetchModelPid(self, staging, schemaName):
#         """ Fetch first model and return pid or else fail.
#         Not all staging areas (SharedStaging) contain a table 'Models', therefore a try/except is used
#         here.
#         """
#         try:
#             modelsResp = staging.query(Path('./graphql-queries/' + 'Models.gql').read_text())
#             if "Models" in modelsResp:
#                 if len(modelsResp['Models']) != 1:
#                     log.warning('Expected a single model in staging area "' + schemaName + '" but found ' + str(len(modelsResp['Models'])))
#                     return None
#             else:
#                 log.warning('Expected a single model in staging area "' + schemaName + '" but found none')
#                 return None

#             return modelsResp['Models'][0]['pid']
#         except KeyError:
#             log.info('Staging area "' + schemaName + '" does not contain a table "Models".')
#             return None

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
        
        if job_strategy == 'FillStaging':
            log.info('Run job strategy: ' + job_strategy)
            Job.sync_fill_staging(self)
        elif job_strategy == 'SharedStaging':
            log.info('Run job strategy: ' + job_strategy)
            Job.sync_shared_staging(self)
        elif job_strategy == 'CohortStagingToDataCatalogue':
            log.info('Run job strategy: ' + job_strategy)
            if Job.get_source_cohort_pid(self):
                Job.sync_cohort_staging_to_datacatalogue(self)
        elif job_strategy == 'NetworkStagingToDataCatalogue':
            log.info('Run job strategy: ' + job_strategy)
            print(Job.get_source_model_pid(self))
            Job.sync_network_staging_to_datacatalogue(self)
        else:
            log.error('Job Strategy not set, please use: FillStaging, SharedStaging, CohortStagingToDataCatalogue')

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
        Job.download_filter_upload(self, tablesToSync)

    def sync_shared_staging(self) -> None:
        """ Sync SOURCE (SharedStaging) with TARGET """
        # order of tables is important, value equals filter
        tablesToSync = {
            'Institutions': None,
            'Contacts': None,
        }
        Job.download_filter_upload(self, tablesToSync)
    
    def sync_cohort_staging_to_datacatalogue(self) -> None:
        # order of tables is important, value equals filter
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
        }

        Job.delete_cohort_from_data_catalogue(self, tablesToSync) # TODO delete does not function
        Job.download_filter_upload(self, tablesToSync)
    
    def sync_network_staging_to_datacatalogue(self) -> None:
        # order of tables is important, value equals filter
        tablesToDelete = {
            'TargetVariableValues': 'variables',
            # 'RepeatedTargetVariables': 'variables',
            # 'TargetVariables': 'variables',
            # 'TargetTables': 'variables',
            # 'TargetDataDictionaries': 'resource',
            # 'CollectionEvents': 'resource',
            # 'Subcohorts': 'resource',
        }

        #Job.delete_cohort_from_data_catalogue(self, tablesToDelete) # TODO delete does not function

        tablesToSync = {
            'TargetVariableValues': None,
            # 'RepeatedTargetVariables': 'variables',
            # 'TargetVariables': 'variables',
            # 'TargetTables': 'variables',
            # 'TargetDataDictionaries': 'resource',
            # 'CollectionEvents': 'resource',
            # 'Subcohorts': 'resource',
        }
        Job.download_filter_upload(self, tablesToSync)
        

    def download_source_data(self, table: str) -> bytes:
        """ Download catalogue data or return None in case of zero rows """
        result = client.Client.query(self.source, 'query Count{' + table + '_agg { count }}')
        if result[table + '_agg']['count'] > 0:
            return client.Client.downLoadCSV(self.source, table)
        return None
    
    def download_filter_upload(self, tablesToSync: dict) -> None:
        """ Download SOURCE csv, filter with pandas and upload csv to TARGET"""
        for table in tablesToSync:
            filter = tablesToSync[table]
            data = Job.download_source_data(self, table)
            
            if data != None:
                df = pd.read_csv(BytesIO(data), dtype='str', na_filter=False) # dtype set to string otherwise numbers will be converted to 2015 -> 2015.0

                stream = StringIO()
                
                if filter != None:
                    df_filter = df[filter] == self.target_database
                    df[df_filter].to_csv(stream, index=False)
                else:
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
                    log.warning('Expected a single model in staging area "' + self.source_database + '" but found ' + str(len(result['Models'])))
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
                variables = {"filter": {"resource": {"equals": [{"pid": Job.get_source_cohort_pid(self)}]}}}
            elif tableType == 'mappings':
                variables = {"filter": {"fromDataDictionary": {"resource": {"equals": [{"pid": Job.get_source_cohort_pid(self)}]}}}}
            elif tableType == 'variables':
                variables = {"filter": {"dataDictionary": {"resource": {"equals": [{"pid": Job.get_source_cohort_pid(self)}]}}}}

            result = self.target.query(query, variables)
        
            if tableName in result:
                #rowsToDelete.append(result[tableName])
                #print(tableName)
                #print(result[tableName])
                # query: "mutation delete($pkey:[CohortsInput]){delete(Cohorts:$pkey){message}}"
                #{pkey: [{pid: "test1"}]}
                #pkey = '[{pid: "' + Job.get_source_cohort_pid(self) + '"}]'
                client.Client.delete(self.target, tableName, result[tableName])
                pass

                # {"query":"mutation delete($pkey:[ContributionsInput])
                # {delete(Contributions:$pkey){message}}",
                # "variables":
                #     {"pkey":
                #         [{"resource":{"pid":"DFBC"},"contact":{"firstName":"Nic","surname":"Timpson"}}]
                #     }
                # }
        