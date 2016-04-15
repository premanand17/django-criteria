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
from criteria.helper.gene_criteria import GeneCriteria

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


class CriteriaDataTest(TestCase):
    '''Criteria Data Integrity test '''

    def compare_marker_results_with_live_site(self):

        # get some random genes from live site with disease tags
        dataset = 'immunobase_criteria_markers'
        # criteria = 'cand_gene_in_study' #done
        # criteria = 'gene_in_region' #done
        criteria = 'is_in_mhc'

        # query gene_criteria with list of ensembl ids
        gene_criteria_index = 'pydgin_imb_criteria_gene'
        # gene_criteria_index_type = 'cand_gene_in_study'
        # gene_criteria_index_type = 'gene_in_region'
        gene_criteria_index_type = 'is_gene_in_mhc'

        # First get the entrez ids
        mart_results = CriteriaDataIntegrityMartUtils.get_mart_results(dataset, criteria, is_phenotag=False)

        entrez_ids = [row['primary_id'] for row in mart_results]
        print('Number of genes fetched from mart for criteria ' + criteria + ' : ' + str(len(entrez_ids)))

        print(entrez_ids)
        entrez2ensembl = self.get_ensemb_ids(entrez_ids)

        entrez_ids_list = [entrez_id for entrez_id in entrez2ensembl]
        entrez_ids_str = ','.join(entrez_ids_list)
        print(entrez_ids_str)

        print('Number of genes after entrez2ensembl ' + str(len(entrez2ensembl)))

        # call phenotag search now
        mart_results = CriteriaDataIntegrityMartUtils.get_mart_results(dataset, criteria, is_phenotag=True,
                                                                       entrez_list=entrez_ids_str)

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

        comparison_result_list = CriteriaDataIntegrityMartUtils.get_comparison_results(gene_criteria_index,
                                                                                       gene_criteria_index_type,
                                                                                       old_criteria_results,
                                                                                       'ensembl_id',
                                                                                       GeneCriteria)

        print('======== Final Result Not matched ==============' + str(len(comparison_result_list)))
        CriteriaDataIntegrityMartUtils.print_results(comparison_result_list)
        print('======== Done ==================================')


    def compare_gene_results_with_live_site(self):

        # get some random genes from live site with disease tags
        dataset = 'immunobase_criteria_genes'
        # criteria = 'cand_gene_in_study' #done
        # criteria = 'gene_in_region' #done
        criteria = 'is_in_mhc'

        # query gene_criteria with list of ensembl ids
        gene_criteria_index = 'pydgin_imb_criteria_gene'
        # gene_criteria_index_type = 'cand_gene_in_study'
        # gene_criteria_index_type = 'gene_in_region'
        gene_criteria_index_type = 'is_gene_in_mhc'

        # First get the entrez ids
        mart_results = CriteriaDataIntegrityMartUtils.get_mart_results(dataset, criteria, is_phenotag=False)

        entrez_ids = [row['primary_id'] for row in mart_results]
        print('Number of genes fetched from mart for criteria ' + criteria + ' : ' + str(len(entrez_ids)))

        print(entrez_ids)
        entrez2ensembl = self.get_ensemb_ids(entrez_ids)

        entrez_ids_list = [entrez_id for entrez_id in entrez2ensembl]
        entrez_ids_str = ','.join(entrez_ids_list)
        print(entrez_ids_str)

        print('Number of genes after entrez2ensembl ' + str(len(entrez2ensembl)))

        # call phenotag search now
        mart_results = CriteriaDataIntegrityMartUtils.get_mart_results(dataset, criteria, is_phenotag=True,
                                                                       entrez_list=entrez_ids_str)

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

        comparison_result_list = CriteriaDataIntegrityMartUtils.get_comparison_results(gene_criteria_index,
                                                                                       gene_criteria_index_type,
                                                                                       old_criteria_results,
                                                                                       'ensembl_id',
                                                                                       GeneCriteria)

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
