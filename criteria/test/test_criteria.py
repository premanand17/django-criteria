from django.test import TestCase
from elastic.elastic_settings import ElasticSettings
import os
import criteria
from data_pipeline.utils import IniParser
from criteria.helper.criteria import Criteria

IDX_SUFFIX = ElasticSettings.getattr('TEST')
MY_INI_FILE = os.path.join(os.path.dirname(__file__), IDX_SUFFIX + '_test_criteria.ini')
TEST_DATA_DIR = os.path.dirname(criteria.__file__) + '/tests/data'
INI_CONFIG = None


def setUpModule():
    ''' Change ini config (MY_INI_FILE) to use the test suffix when
    creating pipeline indices. '''
    ini_file = os.path.join(os.path.dirname(__file__), 'test_criteria.ini')
    if os.path.isfile(MY_INI_FILE):
        return

    with open(MY_INI_FILE, 'w') as new_file:
        with open(ini_file) as old_file:
            for line in old_file:
                new_file.write(line.replace('auto_tests', IDX_SUFFIX))

    global INI_CONFIG
    INI_CONFIG = IniParser().read_ini(MY_INI_FILE)


def tearDownModule():
    # remove index created
    # requests.delete(ElasticSettings.url() + '/' + INI_CONFIG['GENE_HISTORY']['index'])
    os.remove(MY_INI_FILE)


class CriteriaTest(TestCase):
    '''Test interaction staging'''

    def test_get_elastic_query(self):
        config = IniParser().read_ini(MY_INI_FILE)
        section = "is_gene_in_mhc"
        range_query = Criteria.get_elastic_query(section, config)
        range_query_dict = range_query.__dict__
        self.assertTrue('range' in str(range_query_dict))

        section = "cand_gene_in_study"
        match_all_query = Criteria.get_elastic_query(section, config)
        match_all_query_dict = match_all_query.__dict__
        self.assertTrue('match_all' in str(match_all_query_dict))

    def test_get_criteria_dict(self):

        expected_dict = {'fid': 'GDXHsS00004', 'fname': 'Barrett'}
        criteria_dict = Criteria.get_criteria_dict('GDXHsS00004', 'Barrett')
        self.assertEqual(expected_dict, criteria_dict, 'dicts are equal')

        expected_dict = {'fid': 'GDXHsS00004', 'fnotes': {'rsq': '0.1'}, 'fname': 'Barrett'}
        criteria_dict = Criteria.get_criteria_dict('GDXHsS00004', 'Barrett', {'rsq': '0.1'})
        self.assertEqual(expected_dict, criteria_dict, 'dicts are equal')

    def test_get_criteria_disease_dict(self):
        criteria_dict = Criteria.get_criteria_dict('GDXHsS00004', 'Barrett')
        diseases = ['T1D']
        criteria_disease_dict = Criteria.get_criteria_disease_dict(diseases, criteria_dict, {})
        expected_dict = {'T1D': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}]}
        self.assertEqual(criteria_disease_dict, expected_dict, 'Dict as expected')

        criteria_disease_dict = Criteria.get_criteria_disease_dict(diseases, criteria_dict, criteria_disease_dict)
        self.assertEqual(criteria_disease_dict, expected_dict, 'Dict as expected after addding duplicate')

        criteria_dict = Criteria.get_criteria_dict('GDXHsS00005', 'Catfield')
        expected_dict = {'T1D': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'},
                                 {'fname': 'Catfield', 'fid': 'GDXHsS00005'}]}
        criteria_disease_dict = Criteria.get_criteria_disease_dict(diseases, criteria_dict, criteria_disease_dict)
        self.assertEqual(criteria_disease_dict, expected_dict, 'Dict as expected after adding new')

        diseases = ['T1D', 'MS']
        criteria_disease_dict = Criteria.get_criteria_disease_dict(diseases, criteria_dict, criteria_disease_dict)
        expected_dict = {'MS': [{'fname': 'Catfield', 'fid': 'GDXHsS00005'}],
                         'T1D': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'},
                                 {'fname': 'Catfield', 'fid': 'GDXHsS00005'}]}
        self.assertEqual(criteria_disease_dict, expected_dict, 'Dict as expected after adding diseases')

    def test_fetch_overlapping_features(self):
        region_index = ElasticSettings.idx('REGION', idx_type='STUDY_HITS')
        (region_idx, region_idx_type) = region_index.split('/')

        seqid = '1'
        start = 206767602
        stop = 206772494
        result_docs = Criteria.fetch_overlapping_features('38', seqid, start, stop, region_idx, region_idx_type)
        self.assertTrue(len(result_docs) > 0, 'Got some overlapping features')

    def test_calculate_score(self):
        disease_list = ["AA", "T1D"]
        score = Criteria.calculate_score(disease_list)
        self.assertEqual(score, 15, "Got back the right score")

    def test_tag_feature_to_all_diseases(self):
        config = IniParser().read_ini(MY_INI_FILE)
        section = "is_gene_in_mhc"
        feature_id = 'ENSG00000229281'
        result = Criteria.tag_feature_to_all_diseases(feature_id, section, config, {})
        print(result)
