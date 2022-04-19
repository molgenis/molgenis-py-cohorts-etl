# molgenis-py-cohorts-etl

Python library to control data flow from staging environment to live catalogue for cohort rich or descriptive metadata and for variable metadata. Set up of servers and datamodels are found [here](https://github.com/molgenis/molgenis-py-cohorts-etl/datamodels)

## system requirements

- Python 3 (3.8.10)
- Git

## Initial one-time setup

Use virtual env to get a consistent python environment.

1. Clone the github repository

    `git clone git@github.com:molgenis/molgenis-py-cohorts-etl.git`

    `cd molgenis-py-cohorts-etl`

2. Create a virtual python environment

    `python -m venv venv`

3. Activate the virtual python environment

    `source venv/bin/activate`

4. Install the script dependencies from requirements.txt file

    `pip install -r requirements.txt`

More info see:

mac: [https://www.youtube.com/watch?v=Kg1Yvry_Ydk](https://www.youtube.com/watch?v=Kg1Yvry_Ydk)

windows: [https://www.youtube.com/watch?v=APOPm01BVrk](https://www.youtube.com/watch?v=APOPm01BVrk)

## Run the script using docker

(Optionally build the image)

`docker build -t molgenis/molgenis-py-cohorts-etl:latest .`

Run the script ( and remove container when done)

`docker run --rm -it  molgenis/molgenis-py-cohorts-etl:latest`

## Run the script using docker-compose  

Edit the env setting in the docker-compose.yml as needed

`docker-compose up`

## Run the script using the python virtual environment

1. Activate the virtual python environment

    `source venv/bin/activate`

    (1.1 Optionally update the requirements)

    `pip install -r requirements.txt`

2. Configure the script by setting the environment variables

    Add a .env file and fill out the values (see .env-example as a template) or directly setting the values on the system environment.

    | Name        | description  |
    | ------------- | ------------- |
    | MG_JOB_STRATEGY | see [list](/README.md#job-strategies) below for all options |
    | MG_SOURCE_URL | full url of source server |
    | MG_SOURCE_USERNAME | username on source server |
    | MG_SOURCE_PASSWORD | password on source server |
    | MG_SOURCE_DATABASE | single or comma separated list of database names |
    | MG_TARGET_URL | full url of target server |
    | MG_TARGET_USERNAME | username on target server |
    | MG_TARGET_PASSWORD | username on target server |
    | MG_TARGET_DATABASE | single or comma separated list of database names |

    Job strategies

    | Name        | description |
    | ----------- | ----------- |
    | FillStaging | [here](/docs/jobs.md#fillstaging) |
    | SharedStaging | [here](/docs/jobs.md#sharedstaging) |
    | CohortStagingToDataCatalogue | |
    | NetworkStagingToDataCatalogue | |
    | DataCatalogueToNetworkStaging | |
    | UMCGCohorts | |

3. Run the script

    ```python run.py```

4. Deactivate the the virtual python environment

    ```deactivate```

## For script developers

Make sure to update the requirements.txt file if requirements change (added, removed, version change ...)

Git project uses [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) to trigger releases and version updates if needed.
Make sure to use [appropriate commit message format](https://www.conventionalcommits.org/en/v1.0.0/#specification)