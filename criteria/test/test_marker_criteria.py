from django.test import TestCase
from elastic.elastic_settings import ElasticSettings
import os
import criteria
from data_pipeline.utils import IniParser
from criteria.helper.marker_criteria import MarkerCriteria
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

    # create the marker index
    call_command('criteria_index', '--feature', 'marker', '--test')
    Search.index_refresh(INI_CONFIG['DEFAULT']['CRITERIA_IDX_MARKER'])


def tearDownModule():
    # remove index created
    global INI_CONFIG
    requests.delete(ElasticSettings.url() + '/' + INI_CONFIG['DEFAULT']['CRITERIA_IDX_MARKER'])
    os.remove(MY_INI_FILE)


class MarkerCriteriaTest(TestCase):
    '''Test MarkerCriteria'''

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

        self.ic_stats1 = {
            '_index': "hg38_ic_statistics",
            '_type': "uc_liu",
            '_id': "AVNW1YzIV5PGfwclZWaX",
            '_score': 1,
            '_source': {
                'seqid': "1",
                'alt_allele': "G",
                'odds_ratio': 1.1046,
                'lower_or': 1.0695,
                'risk_allele': "A",
                'marker': "rs6697886",
                'raf': 0.137,
                'position': 1238231,
                'p_value': 2.64592978925e-8,
                'imputed': 0,
                'upper_or': 1.1397
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

    def test_marker_is_gwas_significant_in_study(self):
        config = IniParser().read_ini(MY_INI_FILE)
        criteria_results = MarkerCriteria.marker_is_gwas_significant_in_study(self.region_hit2,
                                                                              config=config, result_container={})
        expected_results = {'rs6679677': {'CRO': [{'fname': 'Jostins L',
                                                   'fnotes': {'linkname': 'Jostins L', 'linkvalue': 2.03e-15,
                                                              'linkid': 'GDXHsS00021', 'linkdata': 'pval'},
                                                   'fid': 'GDXHsS00021'}]}}

        self.assertEqual(criteria_results, expected_results, "got expected results")
        criteria_results = MarkerCriteria.marker_is_gwas_significant_in_study(self.region_hit3,
                                                                              config=config, result_container={})
        expected_results = {}
        self.assertEqual(criteria_results, expected_results, "got expected results")

    def test_marker_is_gwas_significant_in_ic(self):
        config = IniParser().read_ini(MY_INI_FILE)
        criteria_results = MarkerCriteria.marker_is_gwas_significant_in_ic(self.ic_stats1,
                                                                           config=config, result_container={})
        marker_id = self.ic_stats1['_source']['marker']
        gw_sig_p = 0.00000005
        criteria_results_fnotes = criteria_results[marker_id]['UC'][0]['fnotes']
        self.assertEqual('pval', criteria_results_fnotes['linkdata'], 'pval is in linkdata')
        p_val_to_compare = float(criteria_results_fnotes['linkvalue'])
        self.assertTrue(p_val_to_compare < gw_sig_p, 'p val less than gwas significant pvalue')

    @override_settings(ELASTIC=OVERRIDE_SETTINGS)
    def test_get_disease_tags(self):
        config = IniParser().read_ini(MY_INI_FILE)
        idx = ElasticSettings.idx('MARKER_CRITERIA')
        available_criterias = MarkerCriteria.get_available_criterias(config=config)['marker']
        idx_type = ','.join(available_criterias)
        doc_by_idx_type = ElasticUtils.get_rdm_docs(idx, idx_type, size=1)
        self.assertTrue(len(doc_by_idx_type) == 1)
        feature_id = getattr(doc_by_idx_type[0], 'qid')

        disease_docs = MarkerCriteria.get_disease_tags(feature_id)

        self.assertIsNotNone(disease_docs, 'got back result docs')
        disease_tags = [getattr(disease_doc, 'code') for disease_doc in disease_docs]
        self.assertIsNotNone(disease_tags, "got back disease tags")

    @override_settings(ELASTIC=OVERRIDE_SETTINGS)
    def test_get_criteria_details(self):
        config = IniParser().read_ini(MY_INI_FILE)
        idx = ElasticSettings.idx('MARKER_CRITERIA')
        available_criterias = MarkerCriteria.get_available_criterias(config=config)['marker']
        idx_type = ','.join(available_criterias)

        doc_by_idx_type = ElasticUtils.get_rdm_docs(idx, idx_type, size=1)
        self.assertTrue(len(doc_by_idx_type) == 1)
        feature_id = getattr(doc_by_idx_type[0], 'qid')

        criteria_details = MarkerCriteria.get_criteria_details(feature_id, config=config)

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
