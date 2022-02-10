from client import Client
from catalogueClient import CatalogueClient
from pathlib import Path
import logging

log = logging.getLogger(__name__)
# TODO: add copying of catalogue from data-catalogue-staging to catalogue on data-catalogue to JobDataCatalogue
# TODO: add more insightful error messages

class Job:
    """
    
    """

    def __init__(self, url, email, password, catalogueDB, sourceDB):
        self.url = url
        self.email = email
        self.password = password
        self.catalogueDB = catalogueDB
        self.sourceDB = sourceDB

        # Client serves as a general exm2 api client
        self.staging = Client(url=self.url, database=sourceDB, email=self.email, password=self.password)
        self.catalogue = Client(url=self.url, database=self.catalogueDB, email=self.email, password=self.password)
        # CatalogueClient serves client specific for use with the catalog model
        self.catalogueClient = CatalogueClient(self.staging, self.catalogue)

    def uploadIfSet(self, table, data):
        """ Upload staging data to catalogue if staging table contains data (else skip) """
        if data is None:
            return
        uploadResp = self.catalogue.uploadCSV(table, data)
        log.info('upload ' + table + ' ; ' + str(uploadResp))

    def download(self, table):
        """ Download staging data or return None in case of zero rows """
        countResp = self.staging.query('query Count{' + table + '_agg { count }}')
        if countResp[table + '_agg']['count'] > 0:
            return self.staging.downLoadCSV(table)
        else:
            return None

    def fetchCohortPid(self, staging, schemaName):
        """ Fetch first cohort and return pid or else fail
        Not all staging areas (network staging areas, SharedStaging) contain a table 'Cohorts',
        therefore a try/except is used here.
        """
        try:
            cohortsResp = staging.query(Path('./graphql-queries/' + 'Cohorts.gql').read_text())
            if "Cohorts" in cohortsResp:
                if len(cohortsResp['Cohorts']) != 1:
                    log.warning('Expected a single cohort in staging area "' + schemaName + '" but found ' + str(
                        len(cohortsResp['Cohorts'])))
                    return None
            else:
                log.warning('Expected a single cohort in staging area "' + schemaName + '" but found none')
                return None

            return cohortsResp['Cohorts'][0]['pid']
        except KeyError:
            log.info('Staging area "' + schemaName + ' does not contain a table "Cohorts".')
            return None

    def fetchModelPid(self, staging, schemaName):
        """ Fetch first model and return pid or else fail.
        Not all staging areas (SharedStaging) contain a table 'Models', therefore a try/except is used
        here.
        """
        try:
            modelsResp = staging.query(Path('./graphql-queries/' + 'Models.gql').read_text())
            if "Models" in modelsResp:
                if len(modelsResp['Models']) != 1:
                    log.warning('Expected a single model in staging area "' + schemaName + '" but found ' + str(len(modelsResp['Models'])))
                    return None
            else:
                log.warning('Expected a single model in staging area "' + schemaName + '" but found none')
                return None

            return modelsResp['Models'][0]['pid']
        except KeyError:
            log.info('Staging area "' + schemaName + '" does not contain a table "Models".')
            return None
