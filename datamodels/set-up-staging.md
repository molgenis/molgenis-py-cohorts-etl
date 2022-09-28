# Set up catalogue with network and cohort staging areas

## Prerequisites

Setup the following schemas and load data
- DataCatalogue (catalogue model)
- CatalogueOntologies (ontologies)
- SharedStaging (stagingShared)

Create a ‘DataCatalogue’ schema by logging in as an admin on your emx2 server. In the ‘Databases’ tab click on the plus sign and fill in ‘DataCatalogue’ as name and select the template ‘DATA_CATALOGUE’. Leave ‘load example data’ as false. By using the template the system should automatically create ‘CatalogueOnlogies’ (will be empty).

Next create ‘ SharedStaging’ in the ‘Databases’ tab by clicking on the blue sign and fill in the name ‘SharedStaging’. You cannot choose a template for this, you need to load the model in the next step. Open ‘SharedStaging’ and open ‘Up/Download’, here you can load the molgenis.csv model which you can find here: https://github.com/molgenis/molgenis-emx2/raw/master/data/datacatalogue/stagingShared/molgenis.csv

After you load the ‘SharedStaging’ model you can load the Contacts and Institutions which need to be shared to all cohorts. You can create your own templates for Contacts and Institution tables by opening the ‘Up/Download’ tab and downloading the csv.

Go back to the ‘Database’ tab and select the schema ‘CatalogueOntologies’, the system created the empty tables and now we can load the data (csv) by uploading https://github.com/molgenis/molgenis-emx2/tree/master/data/datacatalogue/CatalogueOntologies.

# Create cohort staging

Create a ‘COHORT_NAME’ schema by logging in as an admin on your emx2 server. In the ‘Databases’ tab click on the plus sign and fill in COHORT_NAME as name and select the template ‘DATA_CATALOGUE_COHORT_STAGING’. Leave ‘load example data’ as false.

Navigate to the cohort and select the ‘Tables’ tab, open the table ‘Cohorts’ and click on the plus sign. Make sure you fill in the ‘pid’ which should be exactly the same as ‘COHORT_NAME’, this will ensure that the migration script will work properly.

Now you can add cohort editors to let them fill in their data.

# Create UMCG cohort staging

Works as stated above but istead of using a template you need to load the following model: https://github.com/molgenis/molgenis-emx2/raw/master/data/datacatalogue/stagingCohortsUMCG/molgenis.csv.

# Create network staging

Create a NETWORK_NAME schema by logging in as an admin on your emx2 server. In the ‘Databases’ tab click on the plus sign and fill in NETWORK_NAME as name and select the template ‘DATA_CATALOGUE_NETWORK_STAGING’. Leave ‘load example data’ as false.
