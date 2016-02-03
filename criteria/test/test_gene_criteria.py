from django.test import TestCase
from elastic.elastic_settings import ElasticSettings
import os
import criteria
from data_pipeline.utils import IniParser
from criteria.helper.gene_criteria import GeneCriteria

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
    # remove index created
    # requests.delete(ElasticSettings.url() + '/' + INI_CONFIG['GENE_HISTORY']['index'])
    os.remove(MY_INI_FILE)


class GeneCriteriaTest(TestCase):
    '''Test GeneCriteria'''

    def setUp(self):
        '''Runs before each of the tests run from this class..creates the tests/data dir'''
        self.gene_src_full = {
            '_index': "genes_hg38_v0.0.2",
            '_type': "gene",
            '_id': "ENSG00000170439",
            '_score': 1,
            '_source': {
                     'source': 'ensembl_havana',
                     'synonyms': ['ALDI'],
                     'biotype': 'protein_coding',
                     'symbol': 'METTL7B',
                     'suggest': {'weight': 50,
                                 'input': ['ALDI', 'OTTHUMG00000152665', 'ENSG00000170439', '196410', '14458',
                                           '28276', 'METTL7B']
                                 },
                     'stop': 55684611,
                     'description': 'methyltransferase like 7B',
                     'start': 55681546,
                     'chromosome': '12',
                     'dbxrefs': {'trembl': 'A0A087WZT2',
                                 'ensembl': 'ENSG00000170439',
                                 'vega': 'OTTHUMG00000152665',
                                 'entrez': '196410',
                                 'hprd': '14458',
                                 'swissprot': 'Q6UX53',
                                 'orthologs': {'rnorvegicus': {'ensembl': 'ENSRNOG00000007927'},
                                               'mmusculus': {'ensembl': 'ENSMUSG00000025347',
                                                             'MGI': '1918914'}}, 'hgnc': '28276'},
                     'strand': '+',
                     'pmids': ['12477932', '12975309', '15489334', '17004324', '21103663', '26186194']}}

        self.gene_src_position1 = {
            '_index': "genes_hg38_v0.0.2",
            '_type': "gene",
            '_id': "ENSG00000136634",
            '_score': 1,
            '_source': {'_id': 'ENSG00000136634', 'start': 206767602, 'stop': 206772494,
                        'chromosome': '1'}}  # IL10
        self.gene_ensembl1 = {'_id': 'ENSG00000136634'}

        self.region_doc_full = {
                '_index': "regions_v0.0.5",
                '_type': "region",
                '_id': "1p36.12_008",
                '_score': 1,
                '_source': {
                    'tier': 1,
                    'seqid': "1",
                    'disease_loci': [
                        "UC_1005",
                        "IBD_1005"
                    ],
                    'region_name': "1p36.12",
                    'tags': {
                        'disease': [
                            "IBD",
                            "UC"
                        ],
                        'weight': 208
                    },
                    'species': "Human",
                    'region_id': "1p36.12_008"
                    }
                                }

        self.study_doc_full = {'_source': {'diseases': ['RA', 'T1D'],
                                           'genes': ['ENSG00000110800', 'ENSG00000160801', 'ENSG00000163599'],
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

    def test_process_gene_in_region(self):
        ''' Test process_gene_in_region. '''
        config = IniParser().read_ini(MY_INI_FILE)

        # pass a region document
        criteria_results = GeneCriteria.gene_in_region(self.region_doc_full, config=config, result_container={})
        expected_dict = {'ENSG00000279625': {'IBD': [{'fid': '1p36.12_008', 'fname': '1p36.12'}],
                                             'UC': [{'fid': '1p36.12_008', 'fname': '1p36.12'}]}}
        self.assertEqual(criteria_results, expected_dict, 'Got regions in gene as expected')

    def test_cand_gene_in_study(self):
        config = IniParser().read_ini(MY_INI_FILE)

        input_doc = {'_source': {'diseases': ['T1D', 'MS'],
                     'genes': ['ENSG00000110848', 'ENSG00000160791', 'ENSG00000163599'],
                     'study_id': 'GDXHsS00004', 'authors': ['Barrett', 'Type 1 Diabetes Genetics Consortium'],
                     'authors': [
                            {
                             'name': "Barrett",
                             'initials': "JC"
                            },
                            {
                             'name': "Type 1 Diabetes Genetics Consortium",
                             'initials': ""
                            }
                            ]},
                     '_type': 'studies',
                     '_index': 'studies_latest', '_id': 'GDXHsS00004', '_score': 0.0}

        expected_doc = {'ENSG00000110848': {'T1D': [{'fname': 'Barrett JC', 'fid': 'GDXHsS00004'}],
                                            'MS': [{'fname': 'Barrett JC', 'fid': 'GDXHsS00004'}]},
                        'ENSG00000160791': {'T1D': [{'fname': 'Barrett JC', 'fid': 'GDXHsS00004'}],
                                            'MS': [{'fname': 'Barrett JC', 'fid': 'GDXHsS00004'}]},
                        'ENSG00000163599': {'T1D': [{'fname': 'Barrett JC', 'fid': 'GDXHsS00004'}],
                                            'MS': [{'fname': 'Barrett JC', 'fid': 'GDXHsS00004'}]}}

        result_doc = GeneCriteria.cand_gene_in_study(input_doc, config=config, result_container={})

        self.assertEqual(expected_doc, result_doc, 'dicts are equal and as expected')

        input_doc = {'_source': {'diseases': ['RA', 'T1D'],
                     'genes': ['ENSG00000110800', 'ENSG00000160801', 'ENSG00000163599'],
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

        expected_doc = {'ENSG00000160801': {'RA': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield XY'}],
                                            'T1D': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield XY'}]},
                        'ENSG00000163599': {'RA': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield XY'}],
                                            'MS': [{'fid': 'GDXHsS00004', 'fname': 'Barrett JC'}],
                                            'T1D': [{'fid': 'GDXHsS00004', 'fname': 'Barrett JC'},
                                                    {'fid': 'GDXHsS00005', 'fname': 'Clatfield XY'}]},
                        'ENSG00000160791': {'T1D': [{'fname': 'Barrett JC', 'fid': 'GDXHsS00004'}],
                                            'MS': [{'fname': 'Barrett JC', 'fid': 'GDXHsS00004'}]},
                        'ENSG00000110800': {'RA': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield XY'}],
                                            'T1D': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield XY'}]},
                        'ENSG00000110848': {'T1D': [{'fname': 'Barrett JC', 'fid': 'GDXHsS00004'}],
                                            'MS': [{'fname': 'Barrett JC', 'fid': 'GDXHsS00004'}]}}

        updated_doc = GeneCriteria.cand_gene_in_study(input_doc, config=config, result_container=result_doc)
        self.assertEqual(expected_doc, updated_doc, 'dicts are equal and as expected')

        input_doc = {'_source': {'diseases': ['AA'],
                     'genes': ['ENSG00000110900'],
                     'study_id': 'GDXHsS00006', 'authors': [{'name': 'AaTestAuthor', 'initials': 'TT'}]},
                     '_type': 'studies',
                     '_index': 'studies_latest', '_id': 'GDXHsS00006', '_score': 0.0}

        expected_doc = {'ENSG00000160801': {'RA': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield XY'}],
                                            'T1D': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield XY'}]},
                        'ENSG00000163599': {'RA': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield XY'}],
                                            'MS': [{'fid': 'GDXHsS00004', 'fname': 'Barrett JC'}],
                                            'T1D': [{'fid': 'GDXHsS00004', 'fname': 'Barrett JC'},
                                                    {'fid': 'GDXHsS00005', 'fname': 'Clatfield XY'}]},
                        'ENSG00000160791': {'T1D': [{'fname': 'Barrett JC', 'fid': 'GDXHsS00004'}],
                                            'MS': [{'fname': 'Barrett JC', 'fid': 'GDXHsS00004'}]},
                        'ENSG00000110800': {'RA': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield XY'}],
                                            'T1D': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield XY'}]},
                        'ENSG00000110900': {'AA': [{'fid': 'GDXHsS00006', 'fname': 'AaTestAuthor TT'}]},
                        'ENSG00000110848': {'T1D': [{'fname': 'Barrett JC', 'fid': 'GDXHsS00004'}],
                                            'MS': [{'fname': 'Barrett JC', 'fid': 'GDXHsS00004'}]}}

        updated_doc = GeneCriteria.cand_gene_in_study(input_doc, config=config, result_container=result_doc)
        self.assertEqual(expected_doc, updated_doc, 'dicts are equal and as expected')

    def test_tag_feature_to_disease(self):
        ''' Test tag_feature_to_disease. '''
        config = IniParser().read_ini(MY_INI_FILE)
        result1 = GeneCriteria.tag_feature_to_disease(self.region_doc_full, "gene_in_region", config, {})
        # one region tagged to UC and IBD
        expected_result = {'ENSG00000279625': {'IBD': [{'fname': '1p36.12', 'fid': '1p36.12_008'}],
                                               'UC': [{'fname': '1p36.12', 'fid': '1p36.12_008'}]}}
        self.assertEqual(result1, expected_result, 'Got back expected result')

        result2 = GeneCriteria.tag_feature_to_disease(self.study_doc_full, "cand_gene_in_study", config, {})
        expected_result = {'ENSG00000160801': {'RA': [{'fname': 'Clatfield XY', 'fid': 'GDXHsS00005'}],
                                               'T1D': [{'fname': 'Clatfield XY', 'fid': 'GDXHsS00005'}]},
                           'ENSG00000163599': {'RA': [{'fname': 'Clatfield XY', 'fid': 'GDXHsS00005'}],
                                               'T1D': [{'fname': 'Clatfield XY', 'fid': 'GDXHsS00005'}]},
                           'ENSG00000110800': {'RA': [{'fname': 'Clatfield XY', 'fid': 'GDXHsS00005'}],
                                               'T1D': [{'fname': 'Clatfield XY', 'fid': 'GDXHsS00005'}]}}

        self.assertEqual(result2, expected_result, 'Got back expected result')
