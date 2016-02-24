''' Utils for criteria data_integrity '''
import logging
from elastic.search import Search, ElasticQuery
import json
from elastic.elastic_settings import ElasticSettings
from django.test.testcases import TestCase
from criteria.helper.criteria_manager import CriteriaManager
from elastic.utils import ElasticUtils
import requests
from elastic.query import Query
logger = logging.getLogger(__name__)


class CriteriaDataIntegrityTestUtils(TestCase):
    '''Tests common routines'''

    def test_criteria_mappings(self, idx, idx_types):
        (main_codes, other_codes) = CriteriaManager.get_available_diseases()
        site_enabled_diseases = main_codes + other_codes
        elastic_url = ElasticSettings.url()
        for idx_type in idx_types:
            url = idx + '/' + idx_type + '/_mapping'
            response = Search.elastic_request(elastic_url, url, is_post=False)
            elastic_type_mapping = json.loads(response.content.decode("utf-8"))
            property_keys = list(elastic_type_mapping[idx]['mappings'][idx_type]['properties'].keys())
            '''check if score and disease_tags and qid are there in mapping'''
            self.assertIn('score', property_keys)
            self.assertIn('disease_tags', property_keys)
            self.assertIn('qid', property_keys)
            '''check if all the enabled diseases are there'''
            for disease in site_enabled_diseases:
                self.assertIn(disease, property_keys)

    def test_criteria_types(self, idx, idx_types, criterias_from_config):
        '''check if the following criterias are there'''
        for criteria in criterias_from_config:
            self.assertIn(criteria, idx_types)
            doc_count = ElasticUtils.get_docs_count(idx, criteria)
            print(doc_count)
            self.assertGreater(doc_count, 200, 'Criteria doc count greater than 200')


class CriteriaDataIntegrityUtils(object):

    @classmethod
    def get_criteria_index_types(cls, idx_key):

        idx = ElasticSettings.idx(idx_key)
        elastic_url = ElasticSettings.url()
        url = idx + '/_mappings'
        response = Search.elastic_request(elastic_url, url, is_post=False)

        if "error" in response.json():
            logger.warn(response.json())
            return None

        # get idx_types from _mapping
        elastic_mapping = json.loads(response.content.decode("utf-8"))
        idx_types = list(elastic_mapping[idx]['mappings'].keys())
        return idx_types


class CriteriaDataIntegrityMartUtils(object):

    @classmethod
    def get_mart_results(cls, dataset):

        # compare if the disease tags matches with the new criteria results
        mart_url = 'https://mart.immunobase.org/biomart/martservice?'
        limit = 1000

        queryURL = \
            mart_url + \
            'query=<?xml version="1.0" encoding="UTF-8"?>' \
            '<!DOCTYPE Query><Query client="pythonclient" processor="JSON" limit="' + str(limit) + '" header="1">' \
            '<Dataset name="' + dataset + '" config="criteria_config">' \
            '<Attribute name="criteria__object__main__primary_id"/>' \
            '<Attribute name="criteria__object__main__name"/>' \
            '<Attribute name="criteria__object__main__AA_Hs"/>' \
            '<Attribute name="criteria__object__main__AS_Hs"/>' \
            '<Attribute name="criteria__object__main__ATD_Hs"/>' \
            '<Attribute name="criteria__object__main__CEL_Hs"/>' \
            '<Attribute name="criteria__object__main__CRO_Hs"/>' \
            '<Attribute name="criteria__object__main__IBD_Hs"/>' \
            '<Attribute name="criteria__object__main__MS_Hs"/>' \
            '<Attribute name="criteria__object__main__JIA_Hs"/>' \
            '<Attribute name="criteria__object__main__NAR_Hs"/>' \
            '<Attribute name="criteria__object__main__PBC_Hs"/>' \
            '<Attribute name="criteria__object__main__PSC_Hs"/>' \
            '<Attribute name="criteria__object__main__PSO_Hs"/>' \
            '<Attribute name="criteria__object__main__RA_Hs"/>' \
            '<Attribute name="criteria__object__main__SJO_Hs"/>' \
            '<Attribute name="criteria__object__main__SLE_Hs"/>' \
            '<Attribute name="criteria__object__main__SSC_Hs"/>' \
            '<Attribute name="criteria__object__main__T1D_Hs"/>' \
            '<Attribute name="criteria__object__main__UC_Hs"/>' \
            '<Attribute name="criteria__object__main__VIT_Hs"/>' \
            '</Dataset></Query>'
        print(queryURL)
        req = requests.get(queryURL, stream=True, verify=False)
        criteria_json = req.json()

        #         {'T1D Hs score': '55', 'PSO Hs score': '0', 'RA Hs score': '2', 'SJO Hs score': '0',
        #          'MS Hs score': '2', 'VIT Hs score': '0', 'IBD Hs score': '42', 'AA Hs score': '0',
        #          'AS Hs score': '0', 'SSC Hs score': '0', 'CRO Hs score': '58', 'SLE Hs score': '0',
        #          'UC Hs score': '58', 'ATD Hs score': '0', 'JIA Hs score': '0', 'Name': 'ORMDL3',
        #          'PSC Hs score': '0', 'CEL Hs score': '0', 'NAR Hs score': '0', 'PBC Hs score': '68'}
        mart_results = []
        for row in criteria_json['data']:
            processed_row = cls.process_row(row)
            mart_results.append(processed_row)

        return mart_results

    @classmethod
    def process_row(cls, row):
        '''
         row = {'T1D Hs score': '55', 'PSO Hs score': '0', 'RA Hs score': '2', 'SJO Hs score': '0',
           'MS Hs score': '2', 'VIT Hs score': '0', 'IBD Hs score': '42', 'AA Hs score': '0',
           'AS Hs score': '0', 'SSC Hs score': '0', 'CRO Hs score': '58', 'SLE Hs score': '0',
           'UC Hs score': '58', 'ATD Hs score': '0', 'JIA Hs score': '0', 'Name': 'ORMDL3',
           'PSC Hs score': '0', 'CEL Hs score': '0', 'NAR Hs score': '0', 'PBC Hs score': '68', 'Primary id': '94103'}
        '''

        current_row = {}
        current_row['name'] = row['Name']
        current_row['primary_id'] = row['Primary id']

        disease_tags = []
        for key in row:
            if key.endswith('Hs score'):
                dis_code = key.split('Hs score', maxsplit=1)[0].strip()
                if row[key] != '0':
                    disease_tags.append(dis_code)

        current_row['disease_tags'] = disease_tags

        # print(current_row)
        return current_row

    @classmethod
    def get_comparison_results(cls, criteria_idx, old_criteria_results, primary_id_type, criteria_sub_class):
        query = ElasticQuery(Query.ids(list(old_criteria_results.keys())))
        elastic = Search(query, idx=criteria_idx, size=len(old_criteria_results))
        criteria_docs = elastic.search().docs

        print('Length of criteria_docs ' + str(len(criteria_docs)))
        counter = 1
        comparison_result_list = []
        for criteria_doc in criteria_docs:
            print('==========' + str(counter) + '==========')
            print(criteria_doc.__dict__)
            counter = counter + 1
            current_ensembl_id = getattr(criteria_doc, 'qid')
            comparison_result = cls.compare_dicts(criteria_doc.__dict__, old_criteria_results[current_ensembl_id],
                                                  primary_id_type, criteria_sub_class)
            if(len(comparison_result) > 0):
                comparison_result_list.append(comparison_result)

        return comparison_result_list

    @classmethod
    def compare_dicts(cls, new_criteria, old_criteria, primary_id_type, criteria_sub_class):
        #         new_criteria = {'score': 45, 'UC': [{'fname': '16p11.2', 'fid': '16p11.2_007'}],
        #                         'disease_tags': ['IBD', 'MS'],
        #                         '_meta': {'_index': 'pydgin_imb_criteria_gene', '_type': 'gene_in_region',
        #                                    '_id': 'ENSG00000250616', '_score': 1.0},
        #                         'MS': [{'fname': '16p11.2', 'fid': '16p11.2_007'}],
        #                         'PSO': [{'fname': '16p11.2', 'fid': '16p11.2_007'}], 'IBD': [{'fname': '16p11.2',
        #                                'fid': '16p11.2_007'}],
        #                         'qid': 'ENSG00000250616', 'SLE': [{'fname': '16p11.2', 'fid': '16p11.2_007'}]}
        #
        #         old_criteria = {'disease_tags': ['IBD', 'MS'], 'name': 'MARS2', 'primary_id': '92935',
        #                         'ensembl_id': 'ENSG00000250616'}
        comparison_result = {}

        '''REMEMBER: TO CHANGE THE CRITERIA CLASS'''

        if new_criteria['qid'] == old_criteria[primary_id_type]:
            disease_docs = criteria_sub_class.get_disease_tags(new_criteria['qid'])
            new_disease_tags = [getattr(disease_doc, 'code').upper() for disease_doc in disease_docs]
            old_disease_tags = old_criteria['disease_tags']
            if set(new_disease_tags) != set(old_disease_tags):
                comparison_result[new_criteria['qid']] = dict()
                comparison_result[new_criteria['qid']]['new_criteria'] = new_disease_tags
                comparison_result[new_criteria['qid']]['old_criteria'] = old_disease_tags

        return comparison_result

    @classmethod
    def print_results(cls, comparison_result_list, false_only=True):
        disease_exists = True
        counter = 1
        for row in comparison_result_list:
            for ensembl_id in row:
                new_disease_tags = row[ensembl_id]['new_criteria']
                old_disease_tags = row[ensembl_id]['old_criteria']

                for old_dis in old_disease_tags:
                    if old_dis not in new_disease_tags:
                        disease_exists = False
                        break

                if false_only:
                    if disease_exists is False:
                        print(str(counter) + '\t' + ensembl_id + '\t' + str(row[ensembl_id]['new_criteria']) + '\t' +
                              str(row[ensembl_id]['old_criteria']) + '\t' + str(disease_exists))
                        disease_exists = True
                        counter = counter + 1
                else:
                    print(str(counter) + '\t' + ensembl_id + '\t' + str(row[ensembl_id]['new_criteria']) + '\t' +
                          str(row[ensembl_id]['old_criteria']) + '\t' + str(disease_exists))
                    disease_exists = True
                    counter = counter + 1
