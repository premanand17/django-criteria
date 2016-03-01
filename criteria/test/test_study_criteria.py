from django.test import TestCase
from elastic.elastic_settings import ElasticSettings
import os
import criteria
from data_pipeline.utils import IniParser
from criteria.helper.study_criteria import StudyCriteria
import requests
from django.core.management import call_command

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

    # create the study index
    call_command('criteria_index', '--feature', 'study', '--test')


def tearDownModule():
    # remove index created
    global INI_CONFIG
    requests.delete(ElasticSettings.url() + '/' + INI_CONFIG['DEFAULT']['CRITERIA_IDX_STUDY'])
    os.remove(MY_INI_FILE)


class StudyCriteriaTest(TestCase):
    '''Test StudyCriteria'''

    def setUp(self):
        '''Runs before each of the tests run from this class..creates the tests/data dir'''
        self.study_doc_full = {'_source': {'diseases': ['RA', 'T1D'],
                                           'study_id': 'GDXHsS00005',
                                           'authors': [
                            {
                             'name': "Clatfield",
                             'initials': "XY"
                            },
                            {
                             'name': "Type 1 Diabetes Genetics Consortium",
                             'initials': ""
                            }
                            ]},
                          '_type': 'studies',
                          '_index': 'studies_latest', '_id': 'GDXHsS00005', '_score': 0.0}

    def test_study_for_disease(self):

        config = IniParser().read_ini(MY_INI_FILE)
        criteria_results = StudyCriteria.study_for_disease(self.study_doc_full, config=config, result_container={})

        expected_dict = {'GDXHsS00005': {'RA': [{'fid': 'RA', 'fname': 'RA'}], 'T1D': [{'fid': 'T1D', 'fname': 'T1D'}]}}
        self.assertEqual(criteria_results, expected_dict, 'Got result dict for study_for_disease as expected')
