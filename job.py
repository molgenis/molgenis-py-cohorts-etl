from client import Client
from pathlib import Path

class Job:
    """
    
    """

    def __init__(self, url, email, password, stagingDB, catalogueDB):
        self.url = url
        self.email = email
        self.password = password
        self.stagingDB = stagingDB
        self.catalogueDB = catalogueDB

    def sync(self):
        """Sync staging with catalogue"""
        
        print('*** START SYNC STAGING(' + self.stagingDB + ') WITH CATALOGUE (' + self.catalogueDB + ') ***')
        staging = Client(url=self.url, database=self.stagingDB, email=self.email, password=self.password)
        catalogue = Client(url=self.url, database=self.catalogueDB, email=self.email, password=self.password)

        # 1) Query from staging 
        cohorts = Path('cohorts.gql').read_text()
        variables = {'filter': {'name': {'like': 'cohort a'}}}
        cohortsResp = staging.query(cohorts, variables)
        cohorts = []
        if "Cohorts" in cohortsResp:
            cohorts = cohortsResp['Cohorts']
        pids = list(map(lambda cohort: cohort['pid'], cohorts))

        # 2) Delete from catalog
        print('cohorts to delete: ' + ",".join(pids))
        for pid in pids:
            dr = catalogue.delete(table='Cohorts', keyColumn='pid', key=pid)

        # 3) Download from staging
        newData = staging.downLoadCSV('Cohorts')

        # 4) Upload to catalog
        r = catalogue.uploadCSV('Cohorts', newData)

        print('*** END SYNC STAGING(' + self.stagingDB + ') WITH CATALOGUE (' + self.catalogueDB + ') ***')