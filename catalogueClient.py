
from pathlib import Path

class CatalogueClient:
    """
    
    """

    def __init__(self, stagingClient, catalogClient):
        self.stagingClient = stagingClient
        self.catalogClient = catalogClient

    def deleteDocumentationsByCohort(self, cohortPid):
        """delete al documentation from catalog belonging to staging cohort"""

        query = Path('documentation.gql').read_text()
        variables = {"filter": { "resource": { "equals": [{"pid": cohortPid }]}}}
    
        resp = self.stagingClient.query(query, variables)
        rowsToDelete = []
        if "Documentation" in resp:
            rowsToDelete = resp['Documentation']

        for row in rowsToDelete:
            self.catalogClient.delete('Documentation', row)

    def deleteContributionsByCohort(self, cohortPid):
        """delete al contributions from catalog belonging to staging cohort"""

        query = Path('contributions.gql').read_text()
        variables = {"filter": { "resource": { "equals": [{"pid": cohortPid }]}}}

        resp = self.stagingClient.query(query, variables)
        rowsToDelete = []
        if "Contributions" in resp:
            rowsToDelete = resp['Contributions']

        for row in rowsToDelete:
            self.catalogClient.delete('Contributions', row)
    
    def deleteContactsByCohort(self):
        """delete al contacts from catalog that are set in the staging area"""

        query = Path('contacts.gql').read_text()

        resp = self.stagingClient.query(query)
        rowsToDelete = []
        if "Contacts" in resp:
            rowsToDelete = resp['Contacts']

        for row in rowsToDelete:
            self.catalogClient.delete('Contacts', row)

    def deleteCollectionEventsByCohort(self, cohortPid):
        """delete al collectionEvents from catalog belonging to staging cohort"""

        query = Path('collectionEvents.gql').read_text()
        variables = {"filter": { "resource": { "equals": [{"pid": cohortPid }]}}}

        resp = self.stagingClient.query(query, variables)
        rowsToDelete = []
        if "CollectionEvents" in resp:
            rowsToDelete = resp['CollectionEvents']

        for row in rowsToDelete:
            self.catalogClient.delete('CollectionEvents', row)

    def deleteSubcohortsByCohort(self, cohortPid):
        """delete al subCohorts from catalog belonging to staging cohort"""

        query = Path('subcohorts.gql').read_text()
        variables = {"filter": { "resource": { "equals": [{"pid": cohortPid }]}}}

        resp = self.stagingClient.query(query, variables)
        rowsToDelete = []
        if "Subcohorts" in resp:
            rowsToDelete = resp['Subcohorts']

        for row in rowsToDelete:
            self.catalogClient.delete('Subcohorts', row)

    def deletePublicationsByCohort(self):
        """delete all publications from catalog that are set in the staging area"""

        query = Path('publications.gql').read_text()

        resp = self.stagingClient.query(query)
        rowsToDelete = []
        if "Publications" in resp:
            rowsToDelete = resp['Publications']

        for row in rowsToDelete:
            self.catalogClient.delete('Publications', row)


