# molgenis-py-cohorts-etl

Python library to control data flow from staging environment to live catalogue for cohort rich or descriptive metadata and for variable metadata. Set up of servers and datamodels are found [here](https://github.com/molgenis/molgenis-py-cohorts-etl/docs/datamodels).

## system requirements

- Python 3 (3.11.2)
- Git

## Initial one-time setup

Use virtual env to get a consistent python environment.

1. Clone the github repository

    `git clone https://github.com/molgenis/molgenis-py-cohorts-etl.git`

    `cd molgenis-py-cohorts-etl`

2. Create a virtual python environment

    On macOS:

    `python -m venv venv`

    On Linux:

    `python3.11 -m venv venv`
    
    On Windows:
    
    `py -3.11 venv venv `

3. Activate the virtual python environment
    
    On macOS and Linux:

    `source venv/bin/activate`
    
    On Windows:
    
    `.venv\Scripts\activate.bat`

4. Install the script dependencies from requirements.txt file

    `pip install -r requirements.txt`

More info see:

macOS: [https://www.youtube.com/watch?v=Kg1Yvry_Ydk](https://www.youtube.com/watch?v=Kg1Yvry_Ydk)

Windows: [https://www.youtube.com/watch?v=APOPm01BVrk](https://www.youtube.com/watch?v=APOPm01BVrk)

## Run the script using docker

(Optionally build the image)

`docker build -t molgenis/molgenis-py-cohorts-etl:latest .`

Run the script ( and remove container when done)

`docker run --rm -it  molgenis/molgenis-py-cohorts-etl:latest`

## Run the script using docker-compose  

Edit the env setting in the docker-compose.yml as needed

`docker-compose up`

## Run the script using the python virtual environment

1. Configure the script by setting the environment variables

    Add a .env file and fill out the values (see .env-example as a template) or directly setting the values on the system environment.

    | Name                          | Description                                                 | Usage                                                              |
    |-------------------------------|-------------------------------------------------------------|--------------------------------------------------------------------|
    | `MG_JOB_STRATEGY`             | see [list](/README.md#job-strategies) below for all options |                                                                    |
    | `MG_SOURCE_URL`               | full url of source server                                   |                                                                    |
    | `MG_SOURCE_USERNAME`          | username on source server                                   |                                                                    |
    | `MG_SOURCE_PASSWORD`          | password on source server                                   |                                                                    |
    | `MG_SOURCE_DATABASE`          | database name                                               |                                                                    |
    | `MG_TARGET_URL`               | full url of target server                                   |                                                                    |
    | `MG_TARGET_USERNAME`          | username on target server                                   |                                                                    |
    | `MG_TARGET_PASSWORD`          | username on target server                                   |                                                                    |
    | `MG_TARGET_DATABASE`          | single or comma separated list of database names            |                                                                    |
    |   `MG_TARGET_ONTOLOGY`        | json object                                                 | see example below                                                  |

<pre>'{
    "create": [
    {
        "refTable": "Network features",
        "row": "Non-EU institutional research,,"
    },
    {
        "refTable": "Network features",
        "row": "Infectious diseases,,"
    }
    ],
    "update": [
    {
        "refTable": "Network features",
        "column-match": "name",
        "replace": "non-EU institutional research",
        "replace-by": "Non-EU institutional research"
    }
    ],
    "delete": [
    {
        "refTable": "Network features",
        "row": "non-EU institutional research,,"
    }
    ]
}'</pre>

Job strategies

| Name        | Description | SOURCE | TARGET | works with datamodel x.x |
| ----------- | ----------- | ------ | ------ | ------------------------ |
| `COHORT_STAGING_TO_DATA_CATALOGUE_ZIP` | Delete cohort on the TARGET by  `pid`, download and process SOURCE zip and upload to TARGET (make sure schema name is identical to `pid`)| CohortStaging | Catalogue | 3.x |
| `UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP` | Delete cohort on the TARGET by  `pid`, download and process SOURCE zip and upload to TARGET (make sure schema name is identical to `pid`) | UMCG CohortStaging | UMCG Catalogue | 3.x |
| `UMCG_SHARED_ONTOLOGY_ZIP` | Download zip from SOURCE and upload ontology to TARGET (UMCG CatalogueOntologies) | UMCG SharedStaging | UMCG CatalogueOntologies | 2.x |
| `ONTOLOGY_STAGING_TO_DATA_CATALOGUE_ZIP` | Download zip from SOURCE and upload ontology to TARGET  | CatalogueOntologies | CatalogueOntologies | 2.x |
| `NETWORK_STAGING_TO_DATA_CATALOGUE_ZIP` | Download zip from SOURCE and upload ontology to TARGET | NetworkStaging | Catalogue | 3.x |
| `FILL_STAGING` | First create cohort staging area with the correct model, the schema and Cohorts `pid` need to be exactly the same.  Note that files like logo's will not be copied over!| Catalogue | CohortStaging | 2.x |
| `SHARED_STAGING` | Copy SharedStaging model tables, no deletion.| SharedStaging | Catalogue | 2.x |
| `FILL_NETWORK` | First create network staging area with the correct model, the schema and Networks `pid` need to be exactly the same.  Note that files like logo's will not be copied over! | Catalogue | NetworkStaging | 2.x |
| `ONTOLOGY_ETL` | Create, Update, Delete Ontologies from given CatalogueOntologies including all linked Schemas that make use of the ontology | Empty, only TARGET information is used | CatalogueOntologies | 3.x |

2. Run the script

    `python run.py`

    On macOS M2:

    `arch -arm64 python run.py`

3. Deactivate the virtual python environment

    `deactivate`

4. Remove virtual python environment

    On macOS and Linux:

    `rm -rf venv`
    
    On Windows:
    
    `del venv`

## For script developers

Make sure to update the requirements.txt file if requirements change (added, removed, version change ...)

Git project uses [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) to trigger releases and version updates if needed.
Make sure to use [appropriate commit message format](https://www.conventionalcommits.org/en/v1.0.0/#specification)
