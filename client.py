import requests

class Client:
    """
    
    """

    def __init__(self, url, database, email, password):
        self.url = url
        self.database = database
        self.email = email
        self.password = password
        self.session = requests.Session()
        
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
                                
        print(response)
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

        response = self.session.post(self.url + '/' + self.database + '/graphql',
                                 json={'query': query, 'variables': variables}
                                 )
                                
        if response.status_code != 200:
            print(f"Error while posting query, status code {response.status_code}")
            exit()

        responseJson = response.json()

        data = responseJson['data']
        return data

    def delete(self, table, keyColumn='name', key=[]):
        """Delete row by key"""

        query = """
            mutation delete($pkey:String, $password: String) {
                signin(email: $email, password: $password) {
                    status
                    message
                }
            }
        """

        variables = {keyColumn: key}

#         {query: "mutation delete($pkey:[StatusInput]){delete(Status:$pkey){message}}",…}
# query: "mutation delete($pkey:[StatusInput]){delete(Status:$pkey){message}}"
# variables: {pkey: [{name: "foo"}]}
# pkey: [{name: "foo"}]

        

    
