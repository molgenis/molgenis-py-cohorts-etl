from pathlib import Path


class CatalogueClient:
    """
    
    """

    def __init__(self, stagingClient, catalogClient):
        self.stagingClient = stagingClient
        self.catalogClient = catalogClient

    def deleteTableContentsByPid(self, tableName, tableType, pid):
        """delete al documentation from catalogue belonging to cohort"""

        query = Path('./graphql-queries/' + tableName + '.gql').read_text()
        if tableType == 'resource':
            variables = {"filter": {"resource": {"equals": [{"pid": pid}]}}}
        elif tableType == 'mappings':
            variables = {"filter": {"fromDataDictionary": {"resource": {"equals": [{"pid": pid}]}}}}
        elif tableType == 'variables':
            variables = {"filter": {"dataDictionary": {"resource": {"equals": [{"pid": pid}]}}}}

        resp = self.catalogClient.query(query, variables)
        rowsToDelete = []
        if tableName in resp:
            rowsToDelete = resp[tableName]

        for row in rowsToDelete:
            self.catalogClient.delete(tableName, row)
