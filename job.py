from client import Client
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
        
        staging = Client(url=self.url, database=sourceDB, email=self.email, password=self.password)
        catalogue = Client(url=self.url, database=self.catalogueDB, email=self.email, password=self.password)

        # 1) Query from staging 
        cohorts = Path('cohorts.gql').read_text()
        variables = {'filter': {'name': {'like': sourceDB}}}
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