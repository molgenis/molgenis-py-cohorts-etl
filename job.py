from client import Client
from catalogueClient import CatalogueClient
from pathlib import Path
import logging


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

        # Create logger
        self.log = logging.getLogger(__name__)

    def uploadIfSet(self, table, data):
        """ Upload staging data to catalogue if staging table contains data (else skip) """
        if data is None:
            return
        uploadResp = self.catalogue.uploadCSV(table, data)
        self.log.info('upload ' + table + ' ; ' + str(uploadResp))

    def download(self, table):
        """ Download staging data or return None in case of zero rows """
        countResp = self.staging.query('query Count{' + table + '_agg { count }}')
        if countResp[table + '_agg']['count'] > 0:
            return self.staging.downLoadCSV(table)
        else:
            return None

    def fetchCohortPid(self, staging, schemaName):
        """ Fetch first cohort and return pid or else fail """
        cohortsResp = staging.query(Path('Cohorts.gql').read_text())
        if "Cohorts" in cohortsResp:
            if len(cohortsResp['Cohorts']) != 1:
                self.log.error('Expected a single cohort in staging area "' + schemaName + '" but found ' + str(len(cohortsResp['Cohorts'])))
                return None
        else:
            self.log.error('Expected a single cohort in staging area "' + schemaName + '" but found none')
            return None

        return cohortsResp['Cohorts'][0]['pid']

    def fetchModelPid(self, staging, schemaName):
        """ Fetch first cohort and return pid or else fail """
        modelsResp = staging.query(Path('Models.gql').read_text())
        if "Models" in modelsResp:
            if len(modelsResp['Models']) != 1:
                self.log.error('Expected a single model in staging area "' + schemaName + '" but found ' + str(len(modelsResp['Models'])))
                return None
        else:
            self.log.error('Expected a single model in staging area "' + schemaName + '" but found none')
            return None

        return modelsResp['Models'][0]['pid']
