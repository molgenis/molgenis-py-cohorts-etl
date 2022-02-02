#Set up cohort staging areas as follows:

1. Go to an emx2 server

2. Make a schema called CatalogueOntologies, that will contain look-up lists.

3. Make a schema called DataCatalogue and load the [catalogue model](https://github.com/molgenis/molgenis-emx2/blob/master/data/datacatalogue/molgenis.csv)
 that will contain all the catalogue data. The ontology_array and ontology columnTypes will instantiate tables in 
 CatalogueOntologies automatically.

4. Load data in CatalogueOntologies.

5. Load data in DataCatalogue.

6. Make a schema calles SharedStaging and load data.

7. Make separate schemas for each cohort that will enter data. Load [cohort-model.csv](https://github.com/molgenis/molgenis-py-cohorts-etl/datamodels/cohort-model.csv) 
 to each cohort schema to create datamodel for cohort. Fill out cohort pid and name in table 'Cohorts'.

8. Cohort datamanagers can fill out Dictionary.xlsx and Mappings.xlsx templates. Documentation and links to templates are found 
[here](https://data-catalogue.molgeniscloud.org/apps/docs/#/cat_cohort-data-manager).


#Set up Network common data model staging areas:

9. Make schema with name of model, corresponding to the name it has in data-catalogue.molgeniscloud.org (e.g. LifeCycle_CDM)
 Load [network-model.csv](https://github.com/molgenis/molgenis-py-cohorts-etl/datamodels/network-model.csv). Fill out model name in 
table 'Models' manually: pid and name of model.

10. Central data managers of a network can fill out NetworkDictionary.xlsx. Documentation and links to templates are found 
[here](https://data-catalogue.molgeniscloud.org/apps/docs/#/cat_network-data-manager).


# UMCG STAGING

UMCG only fills out Cohorts. The project has a slightly altered staging model, with some data items removed and some descriptions
altered, see [here](https://github.com/molgenis/molgenis-py-cohorts-etl/datamodels/staging-model-umcg.csv)
