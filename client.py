import requests
from pathlib import Path

class Client:
    """
    
    """

    def __init__(self, url, database, email, password):
        self.url = url
        self.database = database
        self.email = email
        self.password = password
        self.session = requests.Session()
        self.graphqlEndpoint = self.url + '/' + self.database + '/graphql'
        self.apiEndpoint = self.url + '/' + self.database + '/api'
        
        self.signin(self.email, self.password)

    def signin(self, email, password):
        """Sign into molgenis and retrieve session cookie"""
        query = """
            mutation($email:String, $password: String) {
                signin(email: $email, password: $password) {
                    status
                    message
                }
            }
        """

        variables = {'email': email, 'password': password}

        response = self.session.post(self.url + '/apps/central/graphql',
                                 json={'query': query, 'variables': variables}
                                 )
                                
        responseJson = response.json()
     
        status = responseJson['data']['signin']['status']
        message = responseJson['data']['signin']['message']

        if status == 'SUCCESS':
            print(f"Success: Signed into {self.database} as {self.email}")
        elif status == 'FAILED':
            print(message)
            exit()
        else:
            print('Error: sign in failed, exiting.')
            exit()

    def query(self, query, variables = {}):
        """Query backend"""

        response = self.session.post(self.graphqlEndpoint,
                                 json={'query': query, 'variables': variables}
                                 )
                                
        if response.status_code != 200:
            print(f"Error while posting query, status code {response.status_code}")
            exit()

        responseJson = response.json()

        data = responseJson['data']
        return data

    def delete(self, table, keyColumn='name', key=''):
        """Delete row by key"""

        query = (""
            "mutation delete($pkey:[" + table + "Input]) {"
                "delete(" + table + ":$pkey){message}"
            "}"
        "")

        variables =  {'pkey': [{keyColumn: key}]}

        response = self.session.post(self.graphqlEndpoint,
                            json={'query': query, 'variables': variables}
                            )

        if response.status_code != 200:
            print(f"Error while deleting, status code {response.status_code}")
            print(response)
            exit()

        return response

    def add(self, table, data={}, draft=False):
        """Add record"""

        query = (""
            "mutation insert($value:[" + table + "Input]) {"
                "insert(" + table + ":$value){message}"
            "}"
        "")

        data['mg_draft'] = draft

        variables = {"value": [data]}

        response = self.session.post(self.graphqlEndpoint,
                    json={'query': query, 'variables': variables}
                    )

        if response.status_code != 200:
            print(f"Error while adding record, status code {response.status_code}")
            print(response)
            exit()

        return response
    
    def fields(self, table): 
        """ Fetch a field list as json array of name value pairs"""

        query = '{__type(name:"' + table + '") {fields { name } } }'

        response = self.session.post(self.graphqlEndpoint, json={'query': query} )

        if response.status_code != 200:
            print(f"Error while fetching table fields, status code {response.status_code}")
            print(response)
            exit()

        return response.json()['data']['__type']['fields']

    def uploadCSV(self, table, data):
        """ Upload csv data ( string ) to table """
        return self.session.post(
            self.apiEndpoint + '/csv/'+ table,
            headers={"Content-Type": 'text/csv'},
            data=data
        )

    def downLoadCSV(self, table):
        """ Download csv data from table """
        resp = self.session.get(self.apiEndpoint + '/csv/'+ table, allow_redirects=True)
        print(resp.headers.get('content-type'))
        if resp.content:
            return resp.content
        else:
            print('Error: download failed')
            exit()

