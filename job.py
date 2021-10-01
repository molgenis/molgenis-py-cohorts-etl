from client import Client
from catalogueClient import CatalogueClient
from pathlib import Path


class Job:
    """
    
    """

    def __init__(self, url, email, password, catalogueDB):
        self.url = url
        self.email = email
        self.password = password
        self.catalogueDB = catalogueDB

    def sync(self, sourceDB):
        """Sync staging with catalogue"""
        
        ## Client serves as a general exm2 api client 
        staging = Client(url=self.url, database=sourceDB, email=self.email, password=self.password)
        catalogue = Client(url=self.url, database=self.catalogueDB, email=self.email, password=self.password)
        ## CatalogueClient serves client specific for use with the catalog model
        catalogueClient = CatalogueClient(staging, catalogue)

        cohortPid = self.__fetchCohortPid(staging, sourceDB)

        # 1) Delete in catalogue
        catalogueClient.deleteDocumentationsByCohort(cohortPid)
        catalogueClient.deleteContributionsByCohort(cohortPid)
        catalogueClient.deleteContactsByCohort(cohortPid)
        catalogueClient.deleteCollectionEventsByCohort(cohortPid)
        catalogue.delete('Cohorts', [{'pid': cohortPid}])

        # # 3) Download from staging
        newCohorts = staging.downLoadCSV('Cohorts')
        newDocumentation = staging.downLoadCSV('Documentation')
        newContacts = staging.downLoadCSV('Contacts')
        newContributions = staging.downLoadCSV('Contributions')
        newCollectionEvents = staging.downLoadCSV('CollectionEvents')
        newSubcohorts = staging.downLoadCSV('Subcohorts')

        # # 4) Upload to catalog
        r = catalogue.uploadCSV('Cohorts', newCohorts)
        r = catalogue.uploadCSV('Documentation', newDocumentation)
        r = catalogue.uploadCSV('Contacts', newContacts)
        r = catalogue.uploadCSV('Contributions', newContributions)
        r = catalogue.uploadCSV('CollectionEvents', newCollectionEvents)
        r = catalogue.uploadCSV('Subcohorts', newSubcohorts)
    
    def __fetchCohortPid(self, staging, schemaName):
        """ fetch first cohort and return pid or else fail """
        cohortsResp = staging.query(Path('cohorts.gql').read_text())
        if "Cohorts" in cohortsResp:
            if len(cohortsResp['Cohorts']) != 1:
                print('Expected a single cohort in stagin area "' + schemaName + '" but found ' + str(len(cohortsResp['Cohorts'])))
                exit(-1)
        else:
            print('Expected a single cohort in stagin area "' + schemaName + '" but found none')
            exit(0)

        return cohortsResp['Cohorts'][0]['pid']
               
