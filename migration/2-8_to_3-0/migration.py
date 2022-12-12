#import util
import os
import shutil
import pathlib
import requests
import time
import pandas as pd
import numpy as np

from zipfile import ZipFile

import client


class Migration:
    def migrate_umcg_catalogue(self) -> None:
        # [done] replace molgenis.csv with version 3.0
        # [done] check molgenis_settings (isChangeLogEnabled == True, isChaptersEnabled == False)
        # [done] molgenis_members
        # Organisations

        dir_source = '../data/SOURCE/' + self.source.database
        dir_target = '../data/TARGET/' + self.source.database
        if os.path.exists(dir_source + '.zip'):
            os.remove(dir_source + '.zip')

        if os.path.exists(dir_target + '/'):
            shutil.rmtree(dir_target + '/')

        result = client.Client.download_zip(self.source)
        
        pathlib.Path(dir_source + '.zip').write_bytes(result)

        with ZipFile(dir_source + '.zip', 'r') as zip:
            for f in zip.namelist():
                if f.startswith('_files/'):
                    zip.extract(f, dir_target + '/')

            zip.extract('molgenis_settings.csv', dir_target + '/')
            zip.extract('molgenis_members.csv', dir_target + '/')
            zip.extract('Institutions.csv', dir_target + '/')
        
        

        open(dir_target + '/molgenis.csv', 'wb').write(requests.get('https://github.com/molgenis/molgenis-emx2/raw/master/data/datacatalogue3/molgenis.csv',allow_redirects=True).content)

        # ##################################
        # molgenis_settings
        # ##################################
        molgenis_settings(
            table = 'molgenis_settings',
            directory = dir_target,
            replace_with = {
                "isChangelogEnabled": "true",
                "isChaptersEnabled": "false"
            }
        )
        # ##################################
        # molgenis.csv
        # ##################################
        molgenis = pd.read_csv(os.path.join(dir_target + '/molgenis.csv'), dtype='str', na_filter=False)
        
        columns_resources = molgenis[
            (molgenis.tableName == 'Resources') & 
            (molgenis.columnType != 'heading') & 
            (molgenis.columnName != '')]['columnName'].values
        columns_organisations = molgenis[
            (molgenis.tableName == 'Organisations') & 
            (molgenis.columnType != 'heading') & 
            (molgenis.columnName != '')]['columnName'].values


        # ##################################
        # [2.8] Institutions -> [3.0] Resources - Organisations - Extended resources
        # ##################################
        result = pd.read_csv(os.path.join(dir_target + '/Institutions.csv'), dtype='str', na_filter=False)

        subset = result[['pid','acronym','name','website','description']]
        subset.insert(0, 'id', subset['pid'], True)
        
        #subset.name = subset.name.fillna(value=subset.id, inplace=True)
        #subset['name'].fillna('er', inplace=True)
        #nan = subset.name.replace('',np.nan,regex=True)
        nan = subset[['name']].apply(lambda x: x.str.strip()).replace('', np.nan)
        nan.name.fillna(subset.id, inplace=True)
        
        #print(nan['name'])
        #new = subset.replace(subset['name'],nan['name'])
        #print(new['name'])
        #subset.replace(subset['name'],nan['name'])
        #subset.replace({'name': {subset['name']: nan['name']}})
        subset = subset.drop(['name'], axis='columns')
        subset.insert(3,'name',nan['name'],True)
        #print(subset)

        subset = subset.drop_duplicates()

        resources = pd.DataFrame(subset, columns=columns_resources)
        resources.to_csv(os.path.join(dir_target,  'Resources.csv'), index=False)

        subset = result[['type','typeOther','name','acronym','country','roles']]
        subset = subset.rename({
            'typeOther':'type other',
            'name':'institution',
            'acronym':'institution acronym',
            'roles':'role'}, axis='columns')
        subset = subset.drop_duplicates()

        organisations = pd.DataFrame(subset, columns=columns_organisations)
        organisations.to_csv(os.path.join(dir_target,  'Organisations.csv'), index=False)


        directory = pathlib.Path(dir_target)
        with ZipFile(dir_target + '.zip', mode="w") as archive:
            for file_path in directory.rglob("*"):
                archive.write(
                    file_path,
                    arcname=file_path.relative_to(directory)
                )
        
        # ##################################
        # delete old tables
        # ##################################


        #upload_zip(self.target, dir_target + '.zip')

def molgenis_settings(
    table: str,
    directory: str,
    replace_with: dict
) -> None:
    result = pd.read_csv(os.path.join(directory, table + '.csv'), dtype='str', na_filter=False)
    
    for r in replace_with:
        result.loc[(result.key == r),'value']=replace_with[r]

    result.to_csv(os.path.join(directory, table + '.csv'), index=False)

# put in util.py
def upload_zip(self, zip) -> None:
        """ Upload zip to SOURCE/TARGET """
        
        response = self.session.post(
            self.apiEndpoint + '/zip?async=true',
            files={'file': open(zip,'rb')}
        )

        def upload_zip_task_status(self, response) -> None:
            task_response = self.session.get(self.url + response.json()['url'])

            if task_response.json()['status'] == 'COMPLETED':
                print(f"{task_response.json()['status']}, {task_response.json()['description']}")
                return

            if task_response.json()['status'] == 'ERROR':
                print(f"{task_response.json()['status']}, {task_response.json()['description']}")
                
            
            if task_response.json()['status'] == 'RUNNING':
                print(f"{task_response.json()['status']}, {task_response.json()['description']}")
                time.sleep(5)
                upload_zip_task_status(self, response)
        
        upload_zip_task_status(self, response)