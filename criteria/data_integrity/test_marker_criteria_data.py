'''Data integrity tests for gene criteria'''
from django.test import TestCase
from elastic.elastic_settings import ElasticSettings
import os
import criteria
from data_pipeline.utils import IniParser

import logging

from criteria.data_integrity.test_utils import CriteriaDataIntegrityTestUtils,\
    CriteriaDataIntegrityUtils, CriteriaDataIntegrityMartUtils
from criteria.helper.criteria import Criteria
from elastic.utils import ElasticUtils
from criteria.helper.marker_criteria import MarkerCriteria

logger = logging.getLogger(__name__)

TEST_DATA_DIR = os.path.dirname(criteria.__file__) + '/tests/data'
global INI_CONFIG
INI_CONFIG = None


def setUpModule():
    ''' Change ini config (MY_INI_FILE) to use the test suffix when
    creating indices. '''
    ini_file = os.path.join(os.path.dirname(__file__), 'criteria.ini')

    global INI_CONFIG
    INI_CONFIG = IniParser().read_ini(ini_file)


def tearDownModule():
    pass


class MarkerCriteriaDataTest(TestCase):
    '''Marker Criteria Data Integrity test '''

    def test_marker_criteria_types(self):
        '''Test if the indexes have records'''
        idx_key = 'MARKER_CRITERIA'
        feature_type = 'marker'
        idx = ElasticSettings.idx(idx_key)

        idx_types = CriteriaDataIntegrityUtils.get_criteria_index_types(idx_key)
        gene_criterias = Criteria.get_available_criterias(feature_type)

        CriteriaDataIntegrityTestUtils().test_criteria_types(idx, idx_types, gene_criterias['gene'])
        CriteriaDataIntegrityTestUtils().test_criteria_mappings(idx, idx_types)

        # get random doc for each type ['gene_in_region', 'cand_gene_in_region', 'cand_gene_in_study', 'is_gene_in_mhc']
        idx_type = 'rsq_with_index_snp'
        doc_by_idx_type = ElasticUtils.get_rdm_docs(idx, idx_type, size=1)
        self.assertTrue(len(doc_by_idx_type) == 1, 'got back one document')
        # TODO check if this region exists in region index and also the gene and disease tag is right
    