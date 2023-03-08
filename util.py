import logging
import os
import pathlib
import zipfile
from io import BytesIO, StringIO
from pathlib import Path

import pandas as pd

import client
import job

log = logging.getLogger(__name__)


class Util:
    @staticmethod
    def download_source_data(source, table: str) -> bytes | None:
        """Download catalogue data or return None in case of zero rows."""
        result = source.query('query Count{' + table + '_agg { count }}')
        if result[table + '_agg']['count'] > 0:
            return source.downLoadCSV(table)
        return None

    @staticmethod
    def download_filter_upload(source: client.Client, target: client.Client,
                               tables_to_sync: dict, network: bool = False) -> None:
        """Download SOURCE csv, filter with pandas and upload csv to TARGET."""
        if network:
            databases = [target.database, target.database + '_CDM']
        else:
            databases = [target.database]

        for table in tables_to_sync.keys():
            table_filter = tables_to_sync.get(table)
            data = Util.download_source_data(source, table)

            if data is None:
                # Skip if there is no data
                continue

            df = pd.read_csv(BytesIO(data), dtype='str',
                             na_filter=False)  # dtype set to string to prevent conversion, e.g. 2015 -> 2015.0

            stream = StringIO()

            for database in databases:
                if table_filter is not None:
                    df_filter = df[table_filter] == database
                    if not df[df_filter].empty:
                        df[df_filter].to_csv(stream, index=False)
                else:
                    df.to_csv(stream, index=False)

                if stream.getvalue():
                    upload_response = target.uploadCSV(
                        table,
                        stream.getvalue().encode('utf-8')
                    )
                    log.info(f'{table}{upload_response.status_code}')

    @staticmethod
    def download_upload(source: client.Client, target: client.Client, tables_to_sync: dict) -> None:
        """Download SOURCE csv and upload csv to TARGET."""
        for table in tables_to_sync.keys():
            data = Util.download_source_data(source, table)

            if data is None:
                # Skip if there is no data
                continue

            df = pd.read_csv(BytesIO(data), dtype='str',
                             na_filter=False)  # dtype set to string to prevent conversion, e.g. 2015 -> 2015.0

            stream = StringIO()

            df.to_csv(stream, index=False)

            upload_response = target.uploadCSV(
                table,
                stream.getvalue().encode('utf-8')
            )
            log.info(f'{table} {upload_response.status_code}')

    @staticmethod
    def get_source_cohort_pid(source: client.Client) -> str | None:
        """Get PID of SOURCE cohort, expects to get one PID, return pid."""
        try:
            result = source.query(Path('./graphql-queries/Cohorts.gql').read_text())
            if "Cohorts" in result:
                if len(result['Cohorts']) != 1:
                    log.warning(
                        f'Expected a single cohort in staging area "{source.database}"'
                        f' but found {len(result["Cohorts"])}')
                    return None
            else:
                log.warning(
                    f'Expected a single cohort in staging area "{source.database}"'
                    f' but found none.')
                return None

            return result['Cohorts'][0]['pid']
        except KeyError:
            log.error(f'Staging area "{source.database} does not contain a table "Cohorts".')
            return None

    @staticmethod
    def get_source_model_pid(source) -> str | None:
        """ get PID of SOURCE network, expects to get one PID, return pid.
        Fetch first model and return pid or else fail.
        Not all staging areas (SharedStaging) contain a table 'Models', therefore a try/except is used
        here.
        """
        result = source.query(Path('./graphql-queries/' + 'Models.gql').read_text())
        try:
            if "Models" in result:
                if len(result['Models']) != 1:
                    log.warning(
                        'Expected a single model in staging area "' + source.database + '" but found ' + str(
                            len(result['Models'])) + ': ' + str(result['Models']))
                    return None
            else:
                log.warning('Expected a single model in staging area "' + source.database + '" but found none')
                return None

            return result['Models'][0]['pid']
        except KeyError:
            log.info('Staging area "' + result + '" does not contain a table "Models".')
            return None

    @staticmethod
    def delete_cohort_from_data_catalogue(source: client.Client, target: client.Client,
                                          tables_to_sync: dict) -> None:
        """Delete SOURCE Cohort data from TARGET data catalogue before upload."""

        source_cohort_pid = Util.get_source_cohort_pid(source)
        if source_cohort_pid is None:
            log.info("No tables to delete.")
            return

        for table_name in tables_to_sync.keys():
            table_type = tables_to_sync.get(table_name)

            query = Path(f'./graphql-queries/{table_name}.gql').read_text()

            if table_type == 'resource':
                variables = {"filter": {"resource": {"equals": [{"pid": source_cohort_pid}]}}}
            elif table_type == 'mappings':
                variables = {"filter": {"fromDataDictionary": {"resource": {"equals": [{"pid": source_cohort_pid}]}}}}
            elif table_type == 'variables':
                variables = {"filter": {"dataDictionary": {"resource": {"equals": [{"pid": source_cohort_pid}]}}}}
            elif table_type == 'pid':
                variables = {"filter": {"equals": [{"pid": source_cohort_pid}]}}
            elif table_type == 'subcohort':
                variables = {"filter": {"subcohort": {"resource": {"equals": [{"pid": source_cohort_pid}]}}}}
            else:
                continue
            result = target.query(query, variables)

            if table_name in result:
                target.delete(table_name, result[table_name])

    # def delete_network_from_data_catalogue(self, tablesToSync: dict) -> None:
    #     """ Delete SOURCE Network data from TARGET data catalogue before upload """
    #     rowsToDelete = []

    #     for tableName in tablesToSync:
    #         tableType = tablesToSync[tableName]

    #         query = Path('./graphql-queries/' + tableName + '.gql').read_text()

    #         if tableType == 'resource':
    #             variables = {"filter": {"resource": {"equals": [{"pid": Util.get_source_model_pid(self)}]}}}
    #         elif tableType == 'mappings':
    #             variables = {"filter": {"fromDataDictionary": {"resource": {"equals": [{"pid": Util.get_source_model_pid(self)}]}}}}
    #         elif tableType == 'variables':
    #             variables = {"filter": {"dataDictionary": {"resource": {"equals": [{"pid": Util.get_source_model_pid(self)}]}}}}

    #         result = self.target.query(query, variables)

    #         if tableName in result:
    #             client.Client.delete(self.target, tableName, result[tableName])

    @staticmethod
    def download_zip_process(source: client.Client, target: client.Client, job_strategy,
                             tablesToSync: dict) -> None:
        """ Download molgenis zip from SOURCE and process zip before upload to TARGET. """
        result = client.Client.download_zip(source)
        # setup output zip stream
        zip_stream = BytesIO()

        try:
            with zipfile.ZipFile(BytesIO(result), mode='r') as archive:
                for name in archive.namelist():
                    if os.path.splitext(name)[0] in tablesToSync:
                        with zipfile.ZipFile(zip_stream, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                            zip_file.writestr(name, BytesIO(archive.read(name)).getvalue())
                    if '_files/' in name:
                        with zipfile.ZipFile(zip_stream, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                            zip_file.writestr(name, BytesIO(archive.read(name)).getvalue())
        except zipfile.BadZipfile as e:
            print(e)

        # Uncomment if you need to debug, will write SOURCE.zip that will be uploaded to TARGET
        # pathlib.Path('SOURCE.ZIP').write_bytes(zip_stream.getvalue())

        if job_strategy == job.JobStrategy.NETWORK_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            client.Client.upload_zip(target, zip_stream)
        elif job_strategy == job.JobStrategy.COHORT_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            client.Client.upload_zip_fallback(target, zip_stream)
        elif job_strategy == job.JobStrategy.UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            client.Client.upload_zip_fallback(target, zip_stream)
        elif job_strategy == job.JobStrategy.UMCG_SHARED_ONTOLOGY_ZIP.name:
            client.Client.upload_zip_fallback(target, zip_stream)
        elif job_strategy == job.JobStrategy.ONTOLOGY_STAGING_TO_DATA_CATALOGUE_ZIP.name:
            client.Client.upload_zip_fallback(target, zip_stream)

    @staticmethod
    def download_target(target: client.Client):
        """ Download target schema as zip, save in case upload fails. """
        filename = 'TARGET.zip'
        if os.path.exists(filename):
            os.remove(filename)

        result = client.Client.download_zip(target)
        pathlib.Path(filename).write_bytes(result)
