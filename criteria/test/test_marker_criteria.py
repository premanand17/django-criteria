from django.test import TestCase
from elastic.elastic_settings import ElasticSettings
import os
import criteria
from data_pipeline.utils import IniParser
from criteria.helper.marker_criteria import MarkerCriteria
from django.core.management import call_command
import requests

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

    # create the gene index
    call_command('criteria_index', '--feature', 'marker', '--test')


def tearDownModule():
    os.remove(MY_INI_FILE)
    # remove index created
    requests.delete(ElasticSettings.url() + '/' + INI_CONFIG['DEFAULT']['CRITERIA_IDX_MARKER'])


class MarkerCriteriaTest(TestCase):
    '''Test GeneCriteria'''

    def setUp(self):
        '''Runs before each of the tests run from this class..creates the tests/data dir'''
        self.region_hit1 = {
            '_index': "regions_v0.0.5",
            '_type': "hits",
            '_id': "AVJkTL1VIXyMwREjq9v4",
            '_source': {
                        'disease_locus': "SLE_X002",
                        'status': "N",
                        'disease': "SLE",
                        'marker': "rs2269368"
            }
            }

        self.region_hit2 = {
            '_index': "regions_v0.0.5",
            '_type': "hits",
            '_id': "AVJkTL1VIXyMwREjq90Z",
            '_source': {
                        'disease_locus': "CRO_1005",
                        'status': "N",
                        'disease': "CRO",
                        'marker': "rs6679677",
                        'dil_study_id': "GDXHsS00021",
                        'p_values': {
                            'combined': "0.00000000000000203",
                            'discovery': "0.000000183",
                            'replication': "0.00000000152"
                        }
            }
            }

        self.region_hit3 = {
            '_index': "regions_v0.0.5",
            '_type': "hits",
            '_id': "AVJkTL1VIXyMwREjq90Z",
            '_source': {
                        'disease_locus': "T1D_1005",
                        'status': "N",
                        'disease': "T1D",
                        'marker': "rs6679677",
                        'dil_study_id': "GDXHsS00001",
                        'p_values': {
                            'combined': "0.00203",
                            'discovery': "0.000000183",
                            'replication': "0.00000000152"
                        }
            }
            }
        self.region_hit4 = {
            '_index': "regions_v0.0.5",
            '_type': "hits",
            '_id': "AVJkTL1VIXyMwREjq90Z",
            '_source': {
                        'disease_locus': "CRO_2004",
                        'status': "N",
                        'disease': "CRO",
                        'marker': "rs10495903",
                        'dil_study_id': "GDXHsS00021"
            }
            }

    def test_is_an_index_snp(self):

        config = IniParser().read_ini(MY_INI_FILE)
        criteria_results = MarkerCriteria.is_an_index_snp(self.region_hit1, config=config, result_container={})
        expected_dict = {'rs2269368': {'SLE': [{'fid': 'Xq28_003', 'fname': 'Xq28'}]}}
        self.assertEqual(criteria_results, expected_dict, 'Got result dict for is_an_index_snp as expected')

    def test_rsq_with_index_snp(self):
        config = IniParser().read_ini(MY_INI_FILE)
        expected_results = {'rs2476601': {'CRO': [{'fname': 'rs6679677',
                                                   'fnotes': {'linkname': 'Jostins L',
                                                              'linkdata': 'rsq',
                                                              'linkid': 'GDXHsS00021', 'linkvalue': 0.97},
                                                   'fid': 'rs6679677'}]}}

        criteria_results = MarkerCriteria.rsq_with_index_snp(self.region_hit2, config=config, result_container={})
        self.assertEqual(expected_results['rs2476601']['CRO'], criteria_results['rs2476601']['CRO'])

        expected_results_ = {'rs2476601': {'T1D': [{'fid': 'rs6679677', 'fname': 'rs6679677',
                                                    'fnotes': {'linkid': 'GDXHsS00001', 'linkdata': 'rsq',
                                                               'linkvalue': 0.97, 'linkname': 'Bradfield JP'}}],
                                           'CRO': [{'fid': 'rs6679677', 'fname': 'rs6679677',
                                                    'fnotes': {'linkid': 'GDXHsS00021', 'linkdata': 'rsq',
                                                               'linkvalue': 0.97, 'linkname': 'Jostins L'}}]}}
        criteria_results_ = MarkerCriteria.rsq_with_index_snp(self.region_hit3, config=config,
                                                              result_container=criteria_results)
        self.assertEqual(expected_results_['rs2476601'], criteria_results_['rs2476601'])

        criteria_results = MarkerCriteria.rsq_with_index_snp(self.region_hit4, config=config, result_container={})
        self.assertIn('rs11904361', criteria_results.keys(), 'rs11904361 found in results')
        self.assertIn('rs6725688', criteria_results.keys(), 'rs6725688 found in results')

    def test_marker_is_gwas_significant(self):
        config = IniParser().read_ini(MY_INI_FILE)
        criteria_results = MarkerCriteria.marker_is_gwas_significant(self.region_hit2,
                                                                     config=config, result_container={})
        expected_results = {'rs6679677': {'CRO': [{'fname': 'Jostins L',
                                                   'fnotes': {'linkname': 'Jostins L', 'linkvalue': 2.03e-15,
                                                              'linkid': 'GDXHsS00021', 'linkdata': 'pval'},
                                                   'fid': 'GDXHsS00021'}]}}

        self.assertEqual(criteria_results, expected_results, "got expected results")
        criteria_results = MarkerCriteria.marker_is_gwas_significant(self.region_hit3,
                                                                     config=config, result_container={})
        expected_results = {}
        self.assertEqual(criteria_results, expected_results, "got expected results")

    def test_get_disease_tags(self):
        disease_docs = MarkerCriteria.get_disease_tags('rs2476601')

        disease_tags = [getattr(disease_doc, 'code') for disease_doc in disease_docs]

        self.assertIn('atd', disease_tags, 'atd in disease_tags')
        self.assertIn('cro', disease_tags, 'cro in disease_tags')
        self.assertIn('sle', disease_tags, 'sle in disease_tags')
