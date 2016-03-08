from django.test import TestCase
from elastic.elastic_settings import ElasticSettings
import os
import criteria
from data_pipeline.utils import IniParser
from criteria.helper.region_criteria import RegionCriteria
from django.core.management import call_command
import requests
from criteria.test.settings_idx import OVERRIDE_SETTINGS
from django.test.utils import override_settings
from elastic.utils import ElasticUtils
from elastic.search import Search

IDX_SUFFIX = ElasticSettings.getattr('TEST')
MY_INI_FILE = os.path.join(os.path.dirname(__file__), IDX_SUFFIX + '_test_criteria.ini')
TEST_DATA_DIR = os.path.dirname(criteria.__file__) + '/tests/data'
INI_CONFIG = None


def setUpModule():
    ''' Change ini config (MY_INI_FILE) to use the test suffix when
    creating pipeline indices. '''
    global INI_CONFIG
    ini_file = os.path.join(os.path.dirname(__file__), 'test_criteria.ini')

    if os.path.isfile(MY_INI_FILE):
        INI_CONFIG = IniParser().read_ini(MY_INI_FILE)
        return

    with open(MY_INI_FILE, 'w') as new_file:
        with open(ini_file) as old_file:
            for line in old_file:
                new_file.write(line.replace('auto_tests', IDX_SUFFIX))

    INI_CONFIG = IniParser().read_ini(MY_INI_FILE)

    # create the region index
    call_command('criteria_index', '--feature', 'region', '--test')
    Search.index_refresh(INI_CONFIG['DEFAULT']['CRITERIA_IDX_REGION'])


def tearDownModule():
    # remove index created
    global INI_CONFIG
    requests.delete(ElasticSettings.url() + '/' + INI_CONFIG['DEFAULT']['CRITERIA_IDX_REGION'])
    os.remove(MY_INI_FILE)


class RegionCriteriaTest(TestCase):
    '''Test RegionCriteria'''

    def setUp(self):
        '''Runs before each of the tests run from this class..creates the tests/data dir'''
        self.region_region1 = {
            '_index': "regions_v0.0.5",
            '_type': "region",
            '_id': "1p36.32_002",
            '_score': 1,
            '_source': {
                        'tier': 1,
                        'seqid': "1",
                        'disease_loci': [
                            "CEL_1001",
                            "MS_1001",
                            "RA_1001",
                            "UC_1002",
                            "ATD_1001",
                            "IBD_1002",
                            "PSC_1001"
                            ],
                        'region_name': "1p36.32",
                        'tags': {
                            'disease': [
                                "RA",
                                "PSC",
                                "ATD",
                                "IBD",
                                "CEL",
                                "MS",
                                "UC"
                                ],
                            'weight': 244
                        },
                        'species': "Human",
                        'region_id': "1p36.32_002"
                        }
                            }

    def test_is_region_for_disease(self):

        config = IniParser().read_ini(MY_INI_FILE)
        criteria_results = RegionCriteria.is_region_for_disease(self.region_region1, config=config, result_container={})

        expected_dict = {'1p36.32_002': {'IBD': [{'fid': '1p36.32_002', 'fname': '1p36.32'}],
                                         'PSC': [{'fid': '1p36.32_002', 'fname': '1p36.32'}],
                                         'CEL': [{'fid': '1p36.32_002', 'fname': '1p36.32'}],
                                         'RA': [{'fid': '1p36.32_002', 'fname': '1p36.32'}],
                                         'UC': [{'fid': '1p36.32_002', 'fname': '1p36.32'}],
                                         'ATD': [{'fid': '1p36.32_002', 'fname': '1p36.32'}],
                                         'MS': [{'fid': '1p36.32_002', 'fname': '1p36.32'}]}}

        self.assertEqual(criteria_results, expected_dict, 'Got result dict for is_region_for_disease as expected')

    def test_is_region_in_mhc(self):

        config = IniParser().read_ini(MY_INI_FILE)
        criteria_results = RegionCriteria.is_region_in_mhc(self.region_region1, config=config, result_container={})
        # should be tagged to all the diseases
        expected_dict = {'1p36.32_002': {'NAR': [{'fname': 'NAR', 'fid': 'NAR'}],
                                         'ATD': [{'fname': 'ATD', 'fid': 'ATD'}],
                                         'PSC': [{'fname': 'PSC', 'fid': 'PSC'}],
                                         'IBD': [{'fname': 'IBD', 'fid': 'IBD'}], 'AA': [{'fname': 'AA', 'fid': 'AA'}],
                                         'JIA': [{'fname': 'JIA', 'fid': 'JIA'}],
                                         'RA': [{'fname': 'RA', 'fid': 'RA'}], 'SLE': [{'fname': 'SLE', 'fid': 'SLE'}],
                                         'SSC': [{'fname': 'SSC', 'fid': 'SSC'}],
                                         'VIT': [{'fname': 'VIT', 'fid': 'VIT'}], 'AS': [{'fname': 'AS', 'fid': 'AS'}],
                                         'UC': [{'fname': 'UC', 'fid': 'UC'}],
                                         'CRO': [{'fname': 'CRO', 'fid': 'CRO'}],
                                         'SJO': [{'fname': 'SJO', 'fid': 'SJO'}],
                                         'MS': [{'fname': 'MS', 'fid': 'MS'}],
                                         'PSO': [{'fname': 'PSO', 'fid': 'PSO'}],
                                         'T1D': [{'fname': 'T1D', 'fid': 'T1D'}],
                                         'PBC': [{'fname': 'PBC', 'fid': 'PBC'}],
                                         'CEL': [{'fname': 'CEL', 'fid': 'CEL'}]}}

        self.assertEqual(criteria_results, expected_dict, 'Got result dict for is_region_in_mhc as expected')

    @override_settings(ELASTIC=OVERRIDE_SETTINGS)
    def test_get_criteria_details(self):
        config = IniParser().read_ini(MY_INI_FILE)
        idx = ElasticSettings.idx('REGION_CRITERIA')
        available_criterias = RegionCriteria.get_available_criterias(config=config)['region']
        idx_type = ','.join(available_criterias)

        doc_by_idx_type = ElasticUtils.get_rdm_docs(idx, idx_type, size=1)
        self.assertTrue(len(doc_by_idx_type) > 0)
        feature_id = getattr(doc_by_idx_type[0], 'qid')

        criteria_details = RegionCriteria.get_criteria_details(feature_id, config=config)

        hits = criteria_details['hits']
        first_hit = hits[0]
        _type = first_hit['_type']
        _index = first_hit['_index']
        _id = first_hit['_id']
        _source = first_hit['_source']

        disease_tag = _source['disease_tags'][0]
        self.assertTrue(feature_id, _id)
        self.assertIn(_type, idx_type)
        self.assertTrue(idx, _index)
        self.assertIn(disease_tag, list(_source.keys()))

        fdetails = _source[disease_tag][0]
        self.assertIn('fid', fdetails.keys())
        self.assertIn('fname', fdetails.keys())
