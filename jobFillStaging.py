import pandas as pd
import logging
from io import BytesIO, StringIO

import job
import client

log = logging.getLogger(__name__)

class JobFillStaging(job.Job):
    """
    Class to copy cohort rich metadata and cohort and network cdm metadata from data catalogue
    to staging areas. Should be executed one time only.
    """

    def __init__(
        self,
        target_url: str,
        target_email: str,
        target_password: str,
        target_database: str,
        source_url: str,
        source_email: str,
        source_password: str,
        source_database: str) -> None:
        ''''''
        self.target_url = target_url
        self.target_email = target_email
        self.target_password = target_password
        self.target_database = target_database
        self.source_url = source_url
        self.source_email = source_email
        self.source_password = source_password
        self.source_database = source_database

        # set up Client for TARGET (aka STAGING)
        self.target = client.Client(
            url = self.target_url,
            database = self.target_database,
            email = self.target_email,
            password = self.target_password
        )

        # set up Client for SOURCE (aka CATALOGUE)
        self.source = client.Client(
            url = self.source_url,
            database = self.source_database,
            email = self.source_email,
            password = self.source_password
        )
 
        self.sync_staging()

    def sync_staging(self):
        """ Sync SOURCE (catalogue) with TARGET (staging)
        """
        def download_source_data(self, table: str):
            """ Download catalogue data or return None in case of zero rows """
            result = client.Client.query(self.source, 'query Count{' + table + '_agg { count }}')
            if result[table + '_agg']['count'] > 0:
                return client.Client.downLoadCSV(self.source, table)
            return None
        
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
        for table in tablesToSync:
            filter = tablesToSync[table]
            data = download_source_data(self, table)
            
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
                print(table, str(uploadResponse))
