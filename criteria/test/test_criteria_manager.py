from django.test import TestCase
from criteria.helper.criteria_manager import CriteriaManager
from elastic.elastic_settings import ElasticSettings
import os
import criteria
from data_pipeline.utils import IniParser
import shutil
from disease import utils

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

    def test_get_criteria_config(self):
        criteria_config = CriteriaManager.get_criteria_config()
        self.assertIsNotNone(criteria_config, 'config is not None')
        section_config = criteria_config['cand_gene_in_study']
        self.assertIsNotNone(section_config, 'section is not None')
        self.assertEqual(section_config['feature'], 'gene', 'Got the right feature')
        self.assertIsNotNone(section_config['desc'], 'Desc is not none')

    def test_get_available_diseases(self):
        (main, other) = utils.Disease.get_site_diseases()
        self.assertEqual(12, len(main), "12 main diseases found when searching for all diseases")
        self.assertEqual(7, len(other), "7 other diseases found when searching for all diseases")

        (main_dis_code, other_dis_code) = CriteriaManager.get_available_diseases()

        self.assertIn('T1D', main_dis_code)
        self.assertIn('AA', other_dis_code)

    def test_available_criterias(self):
        feature = 'gene'
        available_criterias = CriteriaManager.get_available_criterias(feature, INI_CONFIG)
        expected_dict = {'gene': ['cand_gene_in_study', 'gene_in_region', 'is_gene_in_mhc', 'cand_gene_in_region']}
        self.assertIsNotNone(available_criterias, 'Criterias as not none')
        self.assertIn('cand_gene_in_study', available_criterias['gene'])
        self.assertEqual(available_criterias.keys(), expected_dict.keys(), 'Dic keys equal')

        available_criterias = CriteriaManager.get_available_criterias(feature=None, config=INI_CONFIG)
        self.assertIn('gene', available_criterias)
        self.assertIn('marker', available_criterias)

    def test_process_criterias(self):
        feature = 'gene'
        criteria = 'cand_gene_in_study,gene_in_region'
        criteria_list = CriteriaManager.process_criterias(feature, criteria=None, config=None, show=True)
        self.assertIn('cand_gene_in_study', criteria_list, 'cand_gene_in_study in list')
        self.assertIn('is_gene_in_mhc', criteria_list, 'is_gene_in_mhc in list')

        criteria_list = CriteriaManager.process_criterias(feature, criteria=criteria, config=None, show=True)
        self.assertIn('cand_gene_in_study', criteria_list, 'cand_gene_in_study in list')
        self.assertNotIn('is_gene_in_mhc', criteria_list, 'is_gene_in_mhc not in in list')
