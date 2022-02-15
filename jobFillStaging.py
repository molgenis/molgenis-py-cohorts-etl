from client import Client
from catalogueClient import CatalogueClient
import logging
from job import Job


log = logging.getLogger(__name__)


class JobFillStaging(Job):
    """
    Class to copy cohort rich metadata and cohort and network cdm metadata from data catalogue
    to staging areas. Should be executed one time only.

    VRAGEN
    1. Moet PID worden opgevraagd? Of is deze al gedefinieerd in lijstje met targets
    2. Is de volgeorde van tables to sync zoals die moet zijn
    3. Filter stap. (download>filter>upload)
    4. Volgorde upload
    """

    def __init__(self, staging_url, staging_email, staging_password, catalogue_url, catalogue_email, catalogue_password, catalogueDB, targetDB):
        self.staging_url = staging_url
        self.staging_email = staging_email
        self.staging_password = staging_password
        self.catalogue_url = catalogue_url
        self.catalogue_email = catalogue_email
        self.catalogue_password = catalogue_password
        self.catalogueDB = catalogueDB
        self.targetDB = targetDB

        # Client serves as a general exm2 api client
        self.staging = Client(url=self.staging_url, database=targetDB, email=self.staging_email, password=self.staging_password)
        self.catalogue = Client(url=self.catalogue_url, database=self.catalogueDB, email=self.catalogue_email, password=self.catalogue_password)
        # CatalogueClient serves client specific for use with the catalog model
        # self.catalogueClient = CatalogueClient(self.staging, self.catalogue)

        # self.cohortPid = self.fetchCohortPid(self.staging, self.targetDB)

        # self.syncCohort()

    def uploadIfSet(self, table, data):
        """ Upload catalogue data to staging if staging table contains data (else skip) """
        if data is None:
            return
        uploadResp = self.staging.uploadCSV(table, data)
        log.info('upload ' + table + ' ; ' + str(uploadResp))

    def download(self, table):
        """ Download catalogue data or return None in case of zero rows """
        countResp = self.catalogue.query('query Count{' + table + '_agg { count }}')
        if countResp[table + '_agg']['count'] > 0:
            return self.catalogue.downLoadCSV(table)
        else:
            return None

    def syncStaging(self):
        """ Sync catalogue with staging
        """

        tablesToSync = {'VariableMappings': 'mappings',
                        'TableMappings': 'mappings',
                        'SourceVariableValues': 'variables',
                        'RepeatedSourceVariables': 'variables',
                        'SourceVariables': 'variables',
                        'SourceTables': 'variables',
                        'SourceDataDictionaries': 'resource',
                        'Documentation': 'resource',
                        'Contributions': 'resource',
                        'CollectionEvents': 'resource',
                        'Subcohorts': 'resource',
                        'Partners': 'resource'}

        # 2) Download from catalogue
        newVariableMappings = self.download('VariableMappings')
        newTableMappings = self.download('TableMappings')
        newSourceVariableValues = self.download('SourceVariableValues')
        newRepeatedSourceVariables = self.download('RepeatedSourceVariables')
        newSourceVariables = self.download('SourceVariables')
        newSourceTables = self.download('SourceTables')
        newSourceDataDictionaries = self.download('SourceDataDictionaries')
        newCohorts = self.download('Cohorts')
        newDocumentation = self.download('Documentation')
        newContributions = self.download('Contributions')
        newCollectionEvents = self.download('CollectionEvents')
        newSubcohorts = self.download('Subcohorts')
        newPartners = self.download('Partners')
        newPublications = self.download('Publications')


        # 3) Add/Upload to staging
        self.uploadIfSet('Publications', newPublications)
        self.uploadIfSet('Cohorts', newCohorts)
        self.uploadIfSet('Documentation', newDocumentation)
        self.uploadIfSet('Contributions', newContributions)
        self.uploadIfSet('Subcohorts', newSubcohorts)
        self.uploadIfSet('CollectionEvents', newCollectionEvents)
        self.uploadIfSet('Partners', newPartners)
        self.uploadIfSet('SourceDataDictionaries', newSourceDataDictionaries)
        self.uploadIfSet('SourceTables', newSourceTables)
        self.uploadIfSet('SourceVariables', newSourceVariables)
        self.uploadIfSet('RepeatedSourceVariables', newRepeatedSourceVariables)
        self.uploadIfSet('SourceVariableValues', newSourceVariableValues)
        self.uploadIfSet('TableMappings', newTableMappings)
        self.uploadIfSet('VariableMappings', newVariableMappings)
