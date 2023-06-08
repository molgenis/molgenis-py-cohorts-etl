class TablesToSync:
    """The dictionary values are set to None or in case of UMCG cohorts to GDPR.
    If the value is set to GDPR a specific transformation of the table Contacts is performed (Util.process_table)"""
    NETWORK_STAGING_TO_DATA_CATALOGUE_ZIP = {
            'All variables': None,
            'Collection events': None,
            #'Contacts': None,
            'Datasets': None,
            'Documentation': None,
            'Extended resources': None,
            'External identifiers': None,
            'Models': None,
            'Networks': None,
            #'Publications': None,
            'Repeated variables': None,
            'Resources': None,
            'Subcohorts': None,
            'Variable values': None,
            'Variables': None,
        }
    COHORT_STAGING_TO_DATA_CATALOGUE_ZIP = {
            'Variable mappings': None,
            'Variable values': None,
            'Repeated variables': None,
            'Variables': None,
            'Dataset mappings': None,
            'Datasets': None,
            'Documentation': None,
            'Publications': None,
            'Collection events': None,
            'Subcohort counts': None,
            'Subcohorts': None,
            'External identifiers': None,
            'Cohorts': None,
            'Data resources': None,
            'Resources': None,
            'Extended resources': None,
            'Contacts': None
        }
    UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP = {
            'Documentation': None,
            'Publications': None,
            'Collection events': None,
            'Subcohort counts': None,
            'Subcohorts': None,
            'External identifiers': None,
            'Cohorts': None,
            'Data resources': None,
            'Resources': None,
            'Extended resources': None,
            'Contacts': 'GDPR'
        }
    FILL_STAGING = {
            'Publications': None,
            'Cohorts': 'pid',
            'Documentation': 'resource',
            'Contributions': 'resource',
            'Subcohorts': 'resource',
            'CollectionEvents': 'resource',
            'Partners': 'resource',
            'SourceDataDictionaries': 'resource',
            'SourceTables': 'dataDictionary.resource',
            'SourceVariables': 'dataDictionary.resource',
            'RepeatedSourceVariables': 'dataDictionary.resource',
            'SourceVariableValues': 'dataDictionary.resource',
            'TableMappings': 'fromDataDictionary.resource',
            'VariableMappings': 'fromDataDictionary.resource',
        }
    SHARED_STAGING = {
            'Institutions': None,
            'Contacts': None,
        }
    FILL_NETWORK = {
            'Models': 'pid',
            'TargetDataDictionaries': 'resource',
            'TargetTables': 'dataDictionary.resource',
            'Subcohorts': 'resource',
            'CollectionEvents': 'resource',
            'TargetVariables': 'dataDictionary.resource',
            'TargetVariableValues': 'dataDictionary.resource',
            'RepeatedTargetVariables': 'dataDictionary.resource',
        }


class TablesToDelete:
    COHORT_STAGING_TO_DATA_CATALOGUE_ZIP = {
            'VariableMappings': 'mappings',
            'DatasetMappings': 'mappings',
            'VariableValues': 'variables',
            'RepeatedVariables': 'variables',
            'Variables': 'variables',
            'Datasets': 'variables',
            'CollectionEvents': 'resource',
            'Documentation': 'resource',
            'Contacts': 'resource',
            'SubcohortCounts': 'subcohort',
            'Subcohorts': 'resource',
            'ExternalIdentifiers': 'resource',
           # 'ExtendedResources': 'id',
            'DataResources': 'id',
            # 'Publications': None, # doi filter not inplemented
            'Cohorts': 'id'
        }
    UMCG_COHORT_STAGING_TO_DATA_CATALOGUE_ZIP = {
            'Documentation': 'resource',
            'Contacts': 'resource',
            'CollectionEvents': 'resource',
            'SubcohortCounts': 'subcohort',
            'Subcohorts': 'resource',
            'ExternalIdentifiers': 'resource',
            'Cohorts': 'id'
        }


class OntologiesToSync:
    UMCG_SHARED_ONTOLOGY_ZIP = {
            'CoreVariables': None
        }
    ONTOLOGY_STAGING_TO_DATA_CATALOGUE_ZIP = {
            'AgeGroups': None,
            'AreasOfInformation': None,
            'CareSettings': None,
            'CohortDesigns': None,
            'CollectionTypes': None,
            'ContributionTypes': None,
            'CoreVariables': None,
            'Countries': None,
            'DAPsAccessCompleteness': None,
            'DAPsAccessLevels': None,
            'DAPsAccessPermissions': None,
            'DAPsReasonsForAccess': None,
            'DataAccessConditions': None,
            'DataCategories': None,
            'DataUseConditions': None,
            'DatabankFamilies': None,
            'DatasourceTypes': None,
            'Diseases': None,
            'DocumentTypes': None,
            'Formats': None,
            'InformedConsents': None,
            'InstitutionRoles': None,
            'InstitutionTypes': None,
            'Keywords': None,
            'Languages': None,
            'LinkageStrategies': None,
            'Months': None,
            'ObservationTargets': None,
            'PartnerRoles': None,
            'PopulationEntryCauses': None,
            'PopulationExitCauses': None,
            'PopulationSubsets': None,
            'Regions': None,
            'ReleaseTypes': None,
            'ResourceTypes': None,
            'SampleCategories': None,
            'StandardizedTools': None,
            'Status': None,
            'StatusDetails': None,
            'StudyTypes': None,
            'Titles': None,
            'Units': None,
            'Vocabularies': None,
            'Years': None,
        }
