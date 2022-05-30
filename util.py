import zipfile
import pandas as pd
import os
from io import BytesIO, StringIO
from pathlib import Path


import client
import logging

log = logging.getLogger(__name__)

class Util:
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
            data = Util.download_source_data(self, table)
            
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
                log.info(str(table) + ' ' + str(uploadResponse.status_code))
    
    def download_upload(self, tablesToSync: dict) -> None:
        """ Download SOURCE csv and upload csv to TARGET"""
        for table in tablesToSync:
            data = Util.download_source_data(self, table)
            
            if data != None:
                df = pd.read_csv(BytesIO(data), dtype='str', na_filter=False) # dtype set to string otherwise numbers will be converted to 2015 -> 2015.0

                stream = StringIO()
                              
                df.to_csv(stream, index=False)

                uploadResponse = client.Client.uploadCSV(
                    self.target, 
                    table, 
                    stream.getvalue().encode('utf-8')
                )
                log.info(str(table) + ' ' + str(uploadResponse.status_code))
    
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
                variables = {"filter": {"resource": {"equals": [{"pid": Util.get_source_cohort_pid(self)}]}}}
            elif tableType == 'mappings':
                variables = {"filter": {"fromDataDictionary": {"resource": {"equals": [{"pid": Util.get_source_cohort_pid(self)}]}}}}
            elif tableType == 'variables':
                variables = {"filter": {"dataDictionary": {"resource": {"equals": [{"pid": Util.get_source_cohort_pid(self)}]}}}}
            elif tableType == 'pid':
                variables = {"filter": {"equals": [{"pid": Util.get_source_cohort_pid(self)}]}}
                
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
                variables = {"filter": {"resource": {"equals": [{"pid": Util.get_source_model_pid(self)}]}}}
            elif tableType == 'mappings':
                variables = {"filter": {"fromDataDictionary": {"resource": {"equals": [{"pid": Util.get_source_model_pid(self)}]}}}}
            elif tableType == 'variables':
                variables = {"filter": {"dataDictionary": {"resource": {"equals": [{"pid": Util.get_source_model_pid(self)}]}}}}

            result = self.target.query(query, variables)
        
            if tableName in result:
                client.Client.delete(self.target, tableName, result[tableName])
    
    def download_cohort_zip_process(self) -> None:
        """ download molgenis zip from cohort staging area and process zip before upload to datacatalogue """
        result = client.Client.download_zip(self.source)
        result = BytesIO(result)
        result = zipfile.ZipFile(result)

        stream = BytesIO()
        #outputZip = zipfile.ZipFile(stream, 'x')

        # create new stream to setup 'new' zip
        # remove molgenis.csv, molgenis_members.csv
        # - do I need to double check if correct tables are passed?
        # check _files folder (if exists)
        # dirs = list(set([os.path.dirname(x) for x in result.namelist()]))
        # print(dirs)
        # for name in result.namelist():
        #     #print(result.getinfo(name))
        #     print(name)
        # for item in result.infolist():
        #     buffer = result.read(item.filename)
        #     #print(buffer)
        #     if (item.filename == 'Cohorts.csv'):
        #         #outputZip.writestr(item.filename, buffer)
        #         print(item.filename, buffer)
        #         #outputZip.write(item.filename, buffer)
        # outputZip.close()
        #print(outputZip)

        #t = zipfile.ZipFile(outputZip)
        #for name in outputZip.namelist():
        #    print(name)
        # with zipfile.ZipFile(outputZip) as archive:
        #     archive.printdir()
        with zipfile.ZipFile(stream, mode='x', compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr('file.txt', b'derp')
            print(zf)
            for name in zf.namelist():
                print(name)
                print(zf.read(name))

        #print(stream)