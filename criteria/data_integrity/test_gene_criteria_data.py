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
from data_pipeline.helper.gene import Gene

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

    def compare_results_with_live_site(self):

        # get some random genes from live site with disease tags
        dataset = 'immunobase_criteria_genes'
        mart_results = CriteriaDataIntegrityMartUtils.get_mart_results(dataset)

        entrez_ids = [row['primary_id'] for row in mart_results]
        print('Number of genes ' + str(len(entrez_ids)))
        print(entrez_ids)
        entrez2ensembl = self.get_ensemb_ids(entrez_ids)
        print('Number of genes after entrez2ensembl ' + str(len(entrez2ensembl)))

        not_mapped = []
        old_criteria_results = {}
        for row in mart_results:
            if row['primary_id'] in entrez2ensembl:
                row['ensembl_id'] = entrez2ensembl[row['primary_id']]
                old_criteria_results[row['ensembl_id']] = row
            else:
                print('Entrez2ensembl not available for ' + row['primary_id'])
                not_mapped.append(row['primary_id'])

        print('Not mapped ' + str(len(not_mapped)))

        # query gene_criteria with list of ensembl ids
        gene_criteria_index = 'pydgin_imb_criteria_gene'
        comparison_result_list = CriteriaDataIntegrityMartUtils.get_comparison_results(gene_criteria_index,
                                                                                       old_criteria_results,
                                                                                       'ensembl_id')

        print('======== Final Result Not matched ==============' + str(len(comparison_result_list)))
        CriteriaDataIntegrityMartUtils.print_results(comparison_result_list)
        print('======== Done ==================================')

    def get_ensemb_ids(self, entrez_list):
        config = {}
        section = {}
        section['index'] = 'genes_hg38_v0.0.2'
        section['index_type'] = 'gene_history'
        config['GENE_HISTORY'] = section

        result_dict = Gene._entrez_ensembl_lookup(entrez_list, section, config)
        return result_dict
