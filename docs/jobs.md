# Jobs

We use the following jobs to migrate data from SOURCE to TARGET:

EMX2 schemas use predefined datamodels like:
> data-catalogue, cohort-model, network-model, shared-staging, shared-staging-umcg

| Name        | description | SOURCE | TARGET |
| ----------- | ----------- | ------ | ------ |
| COHORT_STAGING_TO_DATA_CATALOGUE_ZIP | Delete cohort on the TARGET by  <code>pid</code>, download and process SOURCE zip and upload to TARGET (make sure schema name is identical to <code>pid</code>)| CohortStaging | Catalogue |
| UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP | Delete cohort on the TARGET by  <code>pid</code>, download and process SOURCE zip and upload to TARGET (make sure schema name is identical to <code>pid</code>) | UMCG CohortStaging | UMCG Catalogue |
| UMCG_SHARED_ONTOLOGY_ZIP | Download zip from SOURCE and upload ontology to TARGET (UMCG CatalogueOntologies) | UMCG SharedStaging | UMCG CatalogueOntologies |
| ONTOLOGY_STAGING_TO_DATA_CATALOGUE_ZIP | Download zip from SOURCE and upload ontology to TARGET  | CatalogueOntologies | CatalogueOntologies |
| NETWORK_STAGING_TO_DATA_CATALOGUE_ZIP | Download zip from SOURCE and upload ontology to TARGET | NetworkStaging | Catalogue |
| FILL_STAGING | First create cohort staging area with the correct model, the schema and Cohorts <code>pid</code> need to be exactly the same.  Note that files like logo's will not be copied over!| Catalogue | CohortStaging |
| SHARED_STAGING | Copy SharedStaging model tables, no deletion.| SharedStaging | Catalogue |
| FILL_NETWORK | First create network staging area with the correct model, the schema and Networks <code>pid</code> need to be exactly the same.  Note that files like logo's will not be copied over! | Catalogue | NetworkStaging |
