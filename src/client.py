import logging
import os
import sys
import time
from io import BytesIO

import requests
from requests import Response

log = logging.getLogger(__name__)


class Client:
    """
    
    """

    def __init__(self, url: str, database: str, email: str, password: str) -> None:
        self.url = url
        self.database = database
        self.email = email
        self.password = password
        self.session = requests.Session()
        self.graphqlEndpoint = f'{self.url}/{self.database}/graphql'
        self.apiEndpoint = f'{self.url}/{self.database}/api'

        self.signin(self.email, self.password)

    def signin(self, email: str, password: str):
        """Sign in to Molgenis and retrieve session cookie."""
        query = """
          mutation($email:String, $password: String) {
            signin(email: $email, password: $password) {
              status
              message
            }
          }
        """

        variables = {'email': email, 'password': password}

        response = self.session.post(
            url=f'{self.url}/apps/central/graphql',
            json={'query': query, 'variables': variables}
        )

        response_json: dict = response.json()

        status: str = response_json['data']['signin']['status']
        message: str = response_json['data']['signin']['message']

        if status == 'SUCCESS':
            log.debug(f"Success: Signed into {self.database} as {self.email}.")
        elif status == 'FAILED':
            log.error(message)
        else:
            log.error('Error: sign in failed, exiting.')

    def query(self, query: str, variables: dict = None) -> dict:
        """Query backend."""

        response = self.session.post(
            url=self.graphqlEndpoint,
            json={"query": query, "variables": variables}
        )

        if response.status_code != 200:
            log.error(f"Error while posting query, status code {response.status_code}")
            # TODO: add logging content > errors > message
            sys.exit()

        return response.json().get('data')

    def delete(self, table: str, pkey: list):
        """Delete row by key."""

        query = (""
                 "mutation delete($pkey:[" + table + "Input]) {\n"
                 "  delete(" + table + ":$pkey){message}\n"
                 "}"
                 "")

        step_size = 1000  # to make sure list is not too big which will make server give error 500

        for i in range(0, len(pkey), step_size):
            variables = {'pkey': pkey[i:i + step_size]}
            response = self.session.post(
                url=self.graphqlEndpoint,
                json={'query': query, 'variables': variables}
            )
            if response.status_code != 200:
                log.error(response)
                log.error(f"Error uploading csv, response.text: {response.text}")

    def add(self, table: str, data, draft=False):
        """Add record."""

        query = (""
                 "mutation insert($value:[" + table + "Input]) {\n"
                 "  insert(" + table + ":$value){message}\n"
                 "}"
                 "")

        data['mg_draft'] = draft

        variables = {"value": [data]}

        response = self.session.post(
            self.graphqlEndpoint,
            json={'query': query, 'variables': variables}
        )

        if response.status_code != 200:
            log.error(f"Error while adding record, status code {response.status_code}")
            log.error(response)

        return response

    def fields(self, table: str):
        """Fetch a field list as json array of name value pairs."""

        query = '{__type(name:"' + table + '") {fields { name } } }'

        response = self.session.post(self.graphqlEndpoint, json={'query': query})

        if response.status_code != 200:
            log.error(f"Error while fetching table fields, status code {response.status_code}.")
            log.error(response)

        return response.json()['data']['__type']['fields']

    def upload_csv(self, table: str, data):
        """Upload csv data ( string ) to table."""
        response = self.session.post(
            url=f'{self.apiEndpoint}/csv/{table}',
            headers={"Content-Type": 'text/csv'},
            data=data
        )

        if response.status_code != 200:
            log.error(response)
            log.error(f"Error uploading csv, status code {response.text}")

        return response

    def download_csv(self, table: str):
        """Download csv data from table."""
        resp = self.session.get(f'{self.apiEndpoint}/csv/{table}', allow_redirects=True)
        if resp.content:
            return resp.content
        else:
            log.error('Error: download failed.')

    def upload_zip(self, data) -> None:
        """Upload zip."""
        response = self.session.post(
            url=f'{self.apiEndpoint}/zip?async=true',
            files={'file': ('zip.zip', data.getvalue())},
        )

        def upload_zip_task_status(_response) -> None:
            task_response = self.session.get(f"{self.url}{_response.json().get('url')}")

            if task_response.json()['status'] == 'COMPLETED':
                log.info(f"{task_response.json().get('status')}, {task_response.json().get('description')}")
                return

            if task_response.json()['status'] == 'ERROR':
                log.error(f"{task_response.json().get('status')}, {task_response.json().get('description')}")
                return

            if task_response.json().get('status') == 'RUNNING':
                log.info(f"{task_response.json().get('status')}, {task_response.json().get('description')}")
                time.sleep(5)
                upload_zip_task_status(_response=_response)

        upload_zip_task_status(_response=response)

    def upload_zip_fallback(self, data: BytesIO) -> None:
        """Upload zip, will fall back on TARGET.zip if upload of SOURCE zip fails."""
        response = self.session.post(
            url=f'{self.apiEndpoint}/zip?async=true',
            files={'file': ('zip.zip', data.getvalue())},
        )

        def upload_zip_task_status(_response: Response) -> None:
            task_response = self.session.get(f"{self.url}{_response.json().get('url')}")

            if task_response.json().get('status') == 'COMPLETED':
                log.info(f"{task_response.json().get('status')}, {task_response.json().get('description')}.")
                filename = '../TARGET.zip'
                if os.path.exists(filename):
                    os.remove(filename)
                return

            if task_response.json().get('status') == 'ERROR':
                log.error(f"{task_response.json().get('status')}, {task_response.json().get('description')}.")
                # Fall back to TARGET.zip
                fallback_response = self.session.post(
                    url=f'{self.apiEndpoint}/zip?async=true',
                    files={'file': open('../TARGET.zip', 'rb')},
                )
                log.info(f"TARGET.zip found, upload zip")
                upload_zip_task_status(_response=fallback_response)  # endless loop ..
                sys.exit()

            if task_response.json()['status'] == 'RUNNING':
                log.info(f"{task_response.json().get('status')}, {task_response.json().get('description')}.")
                time.sleep(5)
                upload_zip_task_status(_response=_response)

        upload_zip_task_status(_response=response)

    def download_zip(self) -> bytes:
        """Download zip data from database."""
        resp = self.session.get(f'{self.apiEndpoint}/zip?includeSystemColumns=true', allow_redirects=True)
        if resp.content:
            return resp.content
        else:
            log.error('Error: download failed')

    def check_database_exists(self) -> None:
        """Check if database exists on server, otherwise complain and exit."""
        query = '{_session {schemas} }'

        response = self.session.post(url=self.graphqlEndpoint, json={'query': query})
        if response.status_code != 200:
            log.error(f"Database schema \'{self.database}\' does not exist, status code {response.status_code}.")
            sys.exit()
