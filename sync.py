import util
from constants import OntologiesToSync, TablesToSync, TablesToDelete


class Sync:
    def fill_staging(self) -> None:
        """ Sync SOURCE (catalogue) with TARGET (staging)
        """       
        # order of tables is important, value equals filter
        tablesToSync = TablesToSync.FILL_STAGING
        util.Util.download_filter_upload(self, tablesToSync)

    def shared_staging(self) -> None:
        """ Sync SOURCE (SharedStaging) with TARGET """
        tablesToSync = TablesToSync.SHARED_STAGING
        util.Util.download_filter_upload(self, tablesToSync)

    def fill_network(self) -> None:
        # order of tables is important, value equals filter
        tablesToSync = TablesToSync.FILL_NETWORK

        util.Util.download_filter_upload(self, tablesToSync, network=True)

    def umcg_cohort_zip_to_datacatalogue(self) -> None:
        """Download UMCGCohortStaging zip, delete cohort on DataCatalogue and finally upload transformed zip"""
        util.Util.download_target(self)
        
        # Database name needs to be identical to cohort PID
        tablesToDelete = TablesToDelete.UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP
        tablesToSync = TablesToSync.UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP
       
        util.Util.delete_cohort_from_data_catalogue(self, tablesToDelete)
        util.Util.download_zip_process(self, tablesToSync)

    def cohort_zip_to_datacatalogue(self) -> None:
        """Download cohort staging zip, delete cohort on DataCatalogue and finally upload transformed zip"""
        util.Util.download_target(self)
        
        # Database name needs to be identical to cohort PID
        tablesToDelete = TablesToDelete.COHORT_STAGING_TO_DATA_CATALOGUE_ZIP
        tablesToSync = TablesToSync.COHORT_STAGING_TO_DATA_CATALOGUE_ZIP
       
        util.Util.delete_cohort_from_data_catalogue(self, tablesToDelete)
        util.Util.download_zip_process(self, tablesToSync)
    
    def network_zip_to_datacatalogue(self) -> None:
        tablesToSync = TablesToSync.NETWORK_STAGING_TO_DATA_CATALOGUE_ZIP
        util.Util.download_zip_process(self, tablesToSync)
    
    def umcg_shared_ontology_zip_to_datacatalogue(self) -> None:
        """ UMCG data model uses SharedStaging Contacts and Institutions directly (same server), no need to sync.
        Upload CoreVariables to CatalogueOntologies as a zip"""
        util.Util.download_target(self)

        ontologiesToSync = OntologiesToSync.UMCG_SHARED_ONTOLOGY_ZIP
        util.Util.download_zip_process(self, ontologiesToSync)
    
    def ontology_staging_zip_to_datacatalogue(self) -> None:
        """ Upload SOURCE CatalogueOntologies to TARGET CatalogueOntologies as a zip"""
        util.Util.download_target(self)

        ontologiesToSync = OntologiesToSync.ONTOLOGY_STAGING_TO_DATA_CATALOGUE_ZIP
        util.Util.download_zip_process(self, ontologiesToSync)
