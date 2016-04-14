import logging
from builtins import classmethod
from criteria.helper.criteria import Criteria
from elastic.elastic_settings import ElasticSettings
from criteria.helper.criteria_manager import CriteriaManager


logger = logging.getLogger(__name__)


class StudyCriteria(Criteria):
    global counter
    counter = 1

    ''' StudyCriteria class define functions for building study criterias, each as separate index types

    '''

    FEATURE_TYPE = 'study'

    @classmethod
    def study_for_disease(cls, hit, section=None, config=None, result_container={}):

        result_container_populated = result_container
        feature_doc = hit['_source']
        feature_doc['_id'] = hit['_id']

        diseases = feature_doc['diseases']
        study_id = feature_doc['study_id']

        for disease in diseases:

            result_container_populated = cls.populate_container(disease,
                                                                disease,
                                                                fnotes=None, features=[study_id],
                                                                diseases=[disease],
                                                                result_container=result_container_populated)
        return result_container_populated

    @classmethod
    def tag_feature_to_disease(cls, feature_doc, section, config, result_container={}):
        feature_class = cls.__name__
        # Get class from globals and create an instance
        m = globals()[feature_class]()
        # Get the function (from the instance) that we need to call
        func = getattr(m, section)
        result_container_ = func(feature_doc, section, config, result_container=result_container)
        return result_container_

    @classmethod
    def get_disease_tags(cls, feature_id, idx_type=None):
        'Function to get disease tags for a given feature_id...delegated to parent class Criteria. Returns disease docs'
        idx = ElasticSettings.idx(cls.FEATURE_TYPE.upper()+'_CRITERIA')
        docs = Criteria.get_disease_tags(feature_id, idx, idx_type)
        return docs

    @classmethod
    def get_disease_tags_as_codes(cls, feature_id):
        '''Function to get disease tags for a given feature_id...delegated to parent class Criteria
        Returns disease codes'''
        disease_docs = cls.get_disease_tags(feature_id)
        disease_codes = [getattr(disease_doc, 'code') for disease_doc in disease_docs]
        return disease_codes

    @classmethod
    def get_disease_codes_from_results(cls, criteria_results):
        idx = ElasticSettings.idx(cls.FEATURE_TYPE.upper()+'_CRITERIA')
        codes = Criteria.get_disease_codes_from_results(idx, criteria_results)
        return sorted(codes)

    @classmethod
    def get_available_criterias(cls, feature=None, config=None):
        'Function to get available criterias for study'
        if config is None:
            config = CriteriaManager.get_criteria_config()

        if feature is None:
            feature = cls.FEATURE_TYPE

        available_criterias = Criteria.get_available_criterias(feature, config)
        return available_criterias

    @classmethod
    def get_criteria_details(cls, feature_id, idx=None, idx_type=None, config=None):
        'Function to get the criteria details for a given feature_id'
        if idx is None:
            idx = ElasticSettings.idx(cls.FEATURE_TYPE.upper()+'_CRITERIA')

        # get all the criterias from ini
        criteria_list = []
        if idx_type is None:
            available_criterias = cls.get_available_criterias(feature=cls.FEATURE_TYPE, config=config)
            criteria_list = available_criterias[cls.FEATURE_TYPE]
            idx_type = ','.join(criteria_list)

        result_dict = Criteria.get_criteria_details(feature_id, idx, idx_type)
        result_dict_expanded = Criteria.add_meta_info(idx, criteria_list, result_dict)

        return result_dict_expanded

    @classmethod
    def get_all_criteria_disease_tags(cls, qids, idx_type=None):

        (idx, idx_types) = cls.get_feature_idx_n_idxtypes(cls.FEATURE_TYPE)

        if idx_type is None:
            idx_type = idx_types

        criteria_disease_tags = Criteria.get_all_criteria_disease_tags(qids, idx, idx_type)
        return(criteria_disease_tags)
