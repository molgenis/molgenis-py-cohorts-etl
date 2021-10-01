from client import Client
from catalogueClient import CatalogueClient


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
        
        staging = Client(url=self.url, database=sourceDB, email=self.email, password=self.password)
        catalogue = Client(url=self.url, database=self.catalogueDB, email=self.email, password=self.password)

        # 1) Delete in catalogue
        cohortPid = sourceDB
        catalogueClient = CatalogueClient(staging, catalogue)

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
               
