import client
import util
from constants import OntologiesToSync, TablesToSync, TablesToDelete


class Sync:
    @staticmethod
    def network_zip_to_datacatalogue(source: client.Client, target: client.Client,
                                     job_strategy: str) -> None:
        tables_to_sync = TablesToSync.NETWORK_STAGING_TO_DATA_CATALOGUE_ZIP
        util.Util.download_zip_process(source=source, target=target, job_strategy=job_strategy,
                                       tables_to_sync=tables_to_sync)

    @staticmethod
    def cohort_zip_to_datacatalogue(source: client.Client, target: client.Client,
                                    job_strategy: str) -> None:
        """Download cohort staging zip, delete cohort on DataCatalogue and finally upload transformed zip."""
        util.Util.download_target(target)

        # Database name needs to be identical to cohort PID
        tables_to_delete = TablesToDelete.COHORT_STAGING_TO_DATA_CATALOGUE_ZIP
        tables_to_sync = TablesToSync.COHORT_STAGING_TO_DATA_CATALOGUE_ZIP

        util.Util.delete_cohort_from_data_catalogue(source=source, target=target,
                                                    tables_to_sync=tables_to_delete)
        util.Util.download_zip_process(source=source, target=target, job_strategy=job_strategy,
                                       tables_to_sync=tables_to_sync)

    @staticmethod
    def umcg_cohort_zip_to_datacatalogue(source: client.Client, target: client.Client,
                                         job_strategy: str) -> None:
        """
        Download UMCGCohortStaging zip, delete cohort on DataCatalogue
        and finally upload transformed zip
        """
        util.Util.download_target(target)

        # Database name needs to be identical to cohort PID
        tables_to_delete = TablesToDelete.UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP
        tables_to_sync = TablesToSync.UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP

        util.Util.delete_cohort_from_data_catalogue(source=source, target=target,
                                                    tables_to_sync=tables_to_delete)
        util.Util.download_zip_process(source=source, target=target, job_strategy=job_strategy,
                                       tables_to_sync=tables_to_sync)

    @staticmethod
    def fill_staging(source: client.Client, target: client.Client) -> None:
        """Sync SOURCE (catalogue) with TARGET (staging)"""

        # The order of tables is important, value equals filter
        tables_to_sync = TablesToSync.FILL_STAGING
        util.Util.download_filter_upload(source=source, target=target,
                                         tables_to_sync=tables_to_sync)

    @staticmethod
    def shared_staging(source: client.Client, target: client.Client) -> None:
        """ Sync SOURCE (SharedStaging) with TARGET """
        tables_to_sync = TablesToSync.SHARED_STAGING
        util.Util.download_filter_upload(source=source, target=target,
                                         tables_to_sync=tables_to_sync)

    @staticmethod
    def umcg_shared_ontology_zip_to_datacatalogue(source: client.Client, target: client.Client,
                                                  job_strategy: str) -> None:
        """
        UMCG data model uses SharedStaging Contacts and Institutions directly (same server), no need to sync.
        Upload CoreVariables to CatalogueOntologies as a zip.
        """
        util.Util.download_target(target)

        ontologies_to_sync = OntologiesToSync.UMCG_SHARED_ONTOLOGY_ZIP
        util.Util.download_zip_process(source=source, target=target, job_strategy=job_strategy,
                                       tables_to_sync=ontologies_to_sync)

    @staticmethod
    def ontology_staging_zip_to_datacatalogue(source: client.Client, target: client.Client,
                                              job_strategy: str) -> None:
        """Upload SOURCE CatalogueOntologies to TARGET CatalogueOntologies as a zip."""
        util.Util.download_target(target)

        ontologies_to_sync = OntologiesToSync.ONTOLOGY_STAGING_TO_DATA_CATALOGUE_ZIP
        util.Util.download_zip_process(source=source, target=target, job_strategy=job_strategy,
                                       tables_to_sync=ontologies_to_sync)

    @staticmethod
    def fill_network(source: client.Client, target: client.Client) -> None:
        # The order of tables is important, value equals filter
        tables_to_sync = TablesToSync.FILL_NETWORK

        util.Util.download_filter_upload(source=source, target=target,
                                         tables_to_sync=tables_to_sync, network=True)
