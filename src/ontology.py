import json
import logging
import pandas as pd
import requests
import sys
log = logging.getLogger(__name__)


class Ontology:
    """ Process ontology specific tasks

    - create ontologies on CatalogueOntologies
    - update ontologies on all Schemas except CatalogueOntologies
    - delete ontologies on CatalogueOntologies"""

    @staticmethod
    def create_ontology(self):
        """Process create ontology on CatalogueOntologies schema (or similar)"""
        data = json.loads(self.ontology)

        for create in data['create']:
            if create.get('rows') is None:
                log.warning(f"Ontology create rows is None")
                return None

            table = create.get('refTable')
            table = Ontology.format_reftable(table)

            data = create.get('rows')
            r = Ontology.add(self, table=table, data=data, draft=False)

            if r.status_code == 200:
                log.info("Ontology create on {} refTable: {}, row: {}".format(self.database, create.get('refTable'),
                                                                              create.get('rows')))
            else:
                log.error(f"Error uploading csv, response.text: {r.text}")

    @staticmethod
    def update_ontology(self):
        """Process update ontology on all Schemas except CatalogueOntologies (self.database)"""
        # fetch datamodel (datacatalogue) molgenis.csv
        datamodel = pd.read_csv(
            "https://raw.githubusercontent.com/molgenis/molgenis-emx2/master/data/datacatalogue/molgenis.csv")
        data = json.loads(self.ontology)

        for update in data['update']:
            result = datamodel.query('`refSchema` == @self.database and `refTable` == @update.get(\'refTable\')')

            if update.get('rows') is None:
                log.warning(f"Ontology update rows is None")
                return None

            if update.get('replace-by') is None:
                log.error(f"Ontology update replace-by is None")
                sys.exit()

            rows = update.get('rows')
            replace_by = update.get('replace-by')

            # return all available schemas on the server
            schemas = self.return_schemas()

            if '_SYSTEM_' in schemas:
                schemas.remove('_SYSTEM_')

            if self.database in schemas:
                schemas.remove(self.database)

            for schema in schemas:
                for i in result.index:
                    table = result['tableName'][i]
                    column = result['columnName'][i]

                    query = (""
                             "query " + table + "($filter:" + table + "Filter) "
                             "{" + table + "(filter:$filter){id,name," + column + "{name}}}"
                             "")
                    variables = {"filter": {column: {"equals": [rows]}}}

                    response = self.session.post(f'{self.url}/{schema}/graphql',
                                                 json={"query": query, "variables": variables})

                    if response.status_code == 200 and response.json()['data']:
                        for j in range(len(response.json()['data'][table])):
                            jid = response.json()['data'][table][j]['id']
                            name = response.json()['data'][table][j]['name']
                            column_data = response.json()['data'][table][j][column]

                            for k in range(len(column_data)):
                                if list(rows.values())[0] in column_data[k].values():
                                    column_data[k][list(rows.keys())[0]] = list(replace_by.values())[0]

                            query2 = (""
                                      "mutation update($value:[" + table + "Input])"
                                      "{update(" + table + ":$value){message}}"
                                      "")
                            variables2 = {"value": [{"id": jid, "name": name, column: column_data}]}
                            response2 = self.session.post(f'{self.url}/{schema}/graphql',
                                                          json={"query": query2, "variables": variables2})
                            if response2.status_code == 200:
                                log.info("Ontology update on schema: {}, table: {}, id: {}, row: {}, replaced-by {}"
                                         .format(schema, table, jid, rows, replace_by))

    @staticmethod
    def delete_ontology(self):
        """Process delete ontology on CatalogueOntologies (or similar)"""
        data = json.loads(self.ontology)

        for delete in data['delete']:
            if delete.get('rows') is None:
                log.warning(f"Ontology delete rows is None")
                return None

            table = delete.get('refTable')
            table = Ontology.format_reftable(table)

            data = delete.get('rows')
            r = Ontology.delete(self, table=table, pkey=data)

            if r.status_code == 200:
                log.info("Ontology delete on {} refTable: {}, row: {}".format(self.database, delete.get('refTable'),
                                                                              delete.get('rows')))
            else:
                log.error(f"Error uploading csv, response.text: {r.text}")

    @staticmethod
    def format_reftable(table: str) -> str:
        """ if refTable is formatted with spaces we need to capitalize each word and join the words without spaces

        For example:
        refTable: 'Network features' is converted to 'NetworkFeatures' """
        if table is None:
            log.error(f"Ontology refTable is None")
            sys.exit()
        result = table.split()
        result = [x.capitalize() for x in result]
        result = ''.join(result)
        return result

    @staticmethod
    def add(self, table: str, data, draft=False) -> requests.Response:
        """Add record. Copy of Client but with slightly different variables string
        make sure single quotes are not passed to graphql"""

        query = (""
                 "mutation insert($value:[" + table + "Input]) {\n"
                 "  insert(" + table + ":$value){message}\n"
                 "}"
                 "")

        data['mg_draft'] = draft
        variables = {"value": [data]}

        response = self.session.post(
            self.graphqlEndpoint,
            json={"query": query, "variables": variables}
        )

        if response.status_code != 200:
            log.error(f"Error while adding record, status code {response.status_code}")
            log.error(response)

        return response

    @staticmethod
    def delete(self, table: str, pkey: list) -> requests.Response:
        """Delete row by key. Copy of client with slightly changed json (double quotes)"""

        query = (""
                 "mutation delete($pkey:[" + table + "Input]) {\n"
                 "  delete(" + table + ":$pkey){message}\n"
                 "}"
                 "")

        variables = {"pkey": [pkey]}
        response = self.session.post(
            url=self.graphqlEndpoint,
            json={"query": query, "variables": variables}
        )
        if response.status_code != 200:
            log.error(f"Error deleting ontology, response.text: {response.text}")
            log.error(response)

        return response
