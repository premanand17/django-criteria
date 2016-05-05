'''Data integrity tests for gene criteria'''
from django.test import TestCase
from elastic.elastic_settings import ElasticSettings
import os
import criteria
from data_pipeline.utils import IniParser

import logging

from criteria.data_integrity.test_utils import CriteriaDataIntegrityTestUtils,\
    CriteriaDataIntegrityUtils
from criteria.helper.criteria import Criteria
from elastic.utils import ElasticUtils

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


class GeneCriteriaDataTest(TestCase):
    '''Gene Criteria Data Integrity test '''

    def test_gene_criteria_types(self):
        '''Test if the indexes have records'''
        idx_key = 'GENE_CRITERIA'
        feature_type = 'gene'
        idx = ElasticSettings.idx(idx_key)

        idx_types = CriteriaDataIntegrityUtils.get_criteria_index_types(idx_key)
        gene_criterias = Criteria.get_available_criterias(feature_type)

        CriteriaDataIntegrityTestUtils().test_criteria_types(idx, idx_types, gene_criterias['gene'])
        CriteriaDataIntegrityTestUtils().test_criteria_mappings(idx, idx_types)

        # get random doc for each type ['gene_in_region', 'cand_gene_in_region', 'cand_gene_in_study', 'is_gene_in_mhc']
        idx_type = 'gene_in_region'
        doc_by_idx_type = ElasticUtils.get_rdm_docs(idx, idx_type, size=1)
        self.assertTrue(len(doc_by_idx_type) == 1, 'got back one document')
        gene_in_region_doc = doc_by_idx_type[0]

#         {'score': 10, 'CRO': [{'fname': '4p11', 'fid': '4p11_005'}],
#          '_meta': {'_type': 'gene_in_region', '_score': 0.9997835,
#                    '_index': 'pydgin_imb_criteria_gene', '_id': 'ENSG00000250753'},
#          'disease_tags': ['CRO'], 'qid': 'ENSG00000250753'}

        qid = getattr(gene_in_region_doc, 'qid')
        print(qid)
        disease_tags = getattr(gene_in_region_doc, 'disease_tags')
#         ENSG00000248482
#         ['IBD', 'UC']
#         [{'fid': '5q31.1_013', 'fname': '5q31.1'}]
#         [{'fid': '5q31.1_013', 'fname': '5q31.1'}]
        fnotes = getattr(gene_in_region_doc, disease_tags[0])
        region_id = fnotes[0]['fid']
        print(region_id)
        # TODO check if this region exists in region index and also the gene and disease tag is right
