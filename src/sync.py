import logging
from src.client import Client
from src.constants import OntologiesToSync, TablesToSync, TablesToDelete
from src.ontology import Ontology
from src.util import Util

log = logging.getLogger(__name__)


class Sync:
    @staticmethod
    def network_zip_to_datacatalogue(source: Client, target: Client,
                                     job_strategy: str) -> None:
        tables_to_sync = TablesToSync.NETWORK_STAGING_TO_DATA_CATALOGUE_ZIP
        Util.download_zip_process(source=source, target=target, job_strategy=job_strategy,
                                  tables_to_sync=tables_to_sync)

    @staticmethod
    def cohort_zip_to_datacatalogue(source: Client, target: Client,
                                    job_strategy: str) -> None:
        """Download cohort staging zip, delete cohort on DataCatalogue and finally upload transformed zip."""
        Util.download_target(target)

        # Database name needs to be identical to cohort PID
        tables_to_delete = TablesToDelete.COHORT_STAGING_TO_DATA_CATALOGUE_ZIP
        tables_to_sync = TablesToSync.COHORT_STAGING_TO_DATA_CATALOGUE_ZIP

        Util.delete_cohort_from_data_catalogue(source=source, target=target,
                                               tables_to_delete=tables_to_delete)
        Util.download_zip_process(source=source, target=target, job_strategy=job_strategy,
                                  tables_to_sync=tables_to_sync)

    @staticmethod
    def umcg_cohort_zip_to_datacatalogue(source: Client, target: Client,
                                         job_strategy: str) -> None:
        """
        Download UMCGCohortStaging zip, delete cohort on DataCatalogue
        and finally upload transformed zip
        """
        Util.download_target(target)

        # Database name needs to be identical to cohort PID
        tables_to_delete = TablesToDelete.UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP
        tables_to_sync = TablesToSync.UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP

        Util.delete_cohort_from_data_catalogue(source=source, target=target,
                                               tables_to_delete=tables_to_delete)
        Util.download_zip_process(source=source, target=target, job_strategy=job_strategy,
                                  tables_to_sync=tables_to_sync)

    @staticmethod
    def fill_staging(source: Client, target: Client) -> None:
        """Sync SOURCE (catalogue) with TARGET (staging)"""

        # The order of tables is important, value equals filter
        tables_to_sync = TablesToSync.FILL_STAGING
        Util.download_filter_upload(source=source, target=target,
                                    tables_to_sync=tables_to_sync)

    @staticmethod
    def shared_staging(source: Client, target: Client) -> None:
        """ Sync SOURCE (SharedStaging) with TARGET """
        tables_to_sync = TablesToSync.SHARED_STAGING
        Util.download_filter_upload(source=source, target=target,
                                    tables_to_sync=tables_to_sync)

    @staticmethod
    def umcg_shared_ontology_zip_to_datacatalogue(source: Client, target: Client,
                                                  job_strategy: str) -> None:
        """
        UMCG data model uses SharedStaging Contacts and Institutions directly (same server), no need to sync.
        Upload CoreVariables to CatalogueOntologies as a zip.
        """
        Util.download_target(target)

        ontologies_to_sync = OntologiesToSync.UMCG_SHARED_ONTOLOGY_ZIP
        Util.download_zip_process(source=source, target=target, job_strategy=job_strategy,
                                  tables_to_sync=ontologies_to_sync)

    @staticmethod
    def ontology_staging_zip_to_datacatalogue(source: Client, target: Client,
                                              job_strategy: str) -> None:
        """Upload SOURCE CatalogueOntologies to TARGET CatalogueOntologies as a zip."""
        Util.download_target(target)

        ontologies_to_sync = OntologiesToSync.ONTOLOGY_STAGING_TO_DATA_CATALOGUE_ZIP
        Util.download_zip_process(source=source, target=target, job_strategy=job_strategy,
                                  tables_to_sync=ontologies_to_sync)

    @staticmethod
    def fill_network(source: Client, target: Client) -> None:
        # The order of tables is important, value equals filter
        tables_to_sync = TablesToSync.FILL_NETWORK

        Util.download_filter_upload(source=source, target=target,
                                    tables_to_sync=tables_to_sync, network=True)

    @staticmethod
    def ontology_etl(self) -> None:
        """ create, update and delete ontology

        """
        # process create ontologies on CatalogueOntologies
        Ontology.create_ontology(self)
        # process update ontologies on all Schemas except CatalogueOntologies
        Ontology.update_ontology(self)
        # process delete ontologies on CatalogueOntologies
        Ontology.delete_ontology(self)
