from client import Client
from catalogueClient import CatalogueClient
from pathlib import Path
import logging

log = logging.getLogger(__name__)

class Job:
    """
    
    """

    def __init__(self, url, email, password, catalogueDB, sourceDB):
        self.url = url
        self.email = email
        self.password = password
        self.catalogueDB = catalogueDB
        self.sourceDB = sourceDB

        ## Client serves as a general exm2 api client 
        self.staging = Client(url=self.url, database=sourceDB, email=self.email, password=self.password)
        self.catalogue = Client(url=self.url, database=self.catalogueDB, email=self.email, password=self.password)
        ## CatalogueClient serves client specific for use with the catalog model
        self.catalogueClient = CatalogueClient(self.staging, self.catalogue)

    def sync(self):
        """Sync staging with catalogue"""
        self.cohortPid = self.__fetchCohortPid(self.staging, self.sourceDB)
        if self.cohortPid is None:
            log.info('Skip sync for: ' + self.sourceDB)

        # 1) Delete in catalogue
        self.catalogueClient.deleteDocumentationsByCohort(self.cohortPid)
        self.catalogueClient.deleteContributionsByCohort(self.cohortPid)
        # self.catalogueClient.deleteContactsByCohort()
        self.catalogueClient.deleteCollectionEventsByCohort(self.cohortPid)
        self.catalogueClient.deletePublicationsByCohort()
        self.catalogueClient.deleteSubcohortsByCohort(self.cohortPid)
        self.catalogueClient.deletePartnersByCohort(self.cohortPid)
        self.catalogue.delete('Cohorts', [{'pid': self.cohortPid}])

        # # 3) Download from staging
        newCohorts = self.__download('Cohorts')
        newDocumentation = self.__download('Documentation')
        newContacts = self.__download('Contacts')
        newContributions = self.__download('Contributions')
        newCollectionEvents = self.__download('CollectionEvents')
        newSubcohorts = self.__download('Subcohorts')
        newPartners = self.__download('Partners')
        newPublications = self.__download('Publications')

        # # 4) Add/Upload to catalog
        self.__uploadIfSet('Publications', newPublications)
        self.__uploadIfSet('Cohorts', newCohorts)
        self.__uploadIfSet('Documentation', newDocumentation)
        self.__uploadIfSet('Contacts', newContacts)
        self.__uploadIfSet('Contributions', newContributions)
        self.__uploadIfSet('Subcohorts', newSubcohorts)
        self.__uploadIfSet('CollectionEvents', newCollectionEvents)
        self.__uploadIfSet('Partners', newPartners)

    def __uploadIfSet(self, table, data):
        """ Upload staging data to catalogue if staging table contains data (else skip) """
        if data is None:
            return
        uploadResp = self.catalogue.uploadCSV(table, data)
        log.info('upload ' + table +' ; ' + str(uploadResp))

    def __download(self, table):
        """ Download staging data or return None in case of zero rows """
        countResp = self.staging.query('query Count{' + table + '_agg { count }}')
        if countResp[table + '_agg']['count'] > 0:
          return self.staging.downLoadCSV(table)
        else:
          return None
    
    def __fetchCohortPid(self, staging, schemaName):
        """ Fetch first cohort and return pid or else fail """
        cohortsResp = staging.query(Path('cohorts.gql').read_text())
        if "Cohorts" in cohortsResp:
            if len(cohortsResp['Cohorts']) != 1:
                log.error('Expected a single cohort in staging area "' + schemaName + '" but found ' + str(len(cohortsResp['Cohorts'])))
                return None
        else:
            log.error('Expected a single cohort in staging area "' + schemaName + '" but found none')
            return None

        return cohortsResp['Cohorts'][0]['pid']
               
