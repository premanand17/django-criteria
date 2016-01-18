from django.test import TestCase
from criteria.helper.criteria_manager import CriteriaManager
from elastic.elastic_settings import ElasticSettings
import os
import criteria
from data_pipeline.utils import IniParser
import shutil

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
    if os.path.exists(TEST_DATA_DIR + '/STAGE'):
        shutil.rmtree(TEST_DATA_DIR + '/STAGE')
    # remove index created
    # requests.delete(ElasticSettings.url() + '/' + INI_CONFIG['GENE_HISTORY']['index'])
    os.remove(MY_INI_FILE)


class CriteriaManagerTest(TestCase):
    '''Test CriteriaManager functions'''

    def test_available_criterias(self):
        feature = 'gene'
        available_criterias = CriteriaManager.get_available_criterias(feature, INI_CONFIG)
        expected_dict = {'gene': ['cand_gene_in_study', 'gene_in_region']}
        self.assertEqual(expected_dict, available_criterias, 'Criterias as expected with feature')

        available_criterias = CriteriaManager.get_available_criterias(feature=None, config=INI_CONFIG)
        expected_dict = {'marker': ['is_an_index_snp'], 'gene': ['cand_gene_in_study', 'gene_in_region']}
        self.assertEqual(expected_dict, available_criterias, 'Criterias as expected without feature')

    def test_process_criterias(self):
        feature = 'gene'
        criteria = 'cand_gene_in_study, blablabbla, gene_in_region'
        CriteriaManager.process_criterias(feature, criteria)
