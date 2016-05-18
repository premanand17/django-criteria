from django.test import TestCase
from elastic.elastic_settings import ElasticSettings
import os
import criteria
from data_pipeline.utils import IniParser
from criteria.helper.gene_criteria import GeneCriteria
from django.test.utils import override_settings
from elastic.utils import ElasticUtils
import disease.document
from pydgin.tests.data.settings_idx import PydginTestSettings


IDX_SUFFIX = ElasticSettings.getattr('TEST')
MY_INI_FILE = os.path.join(os.path.dirname(__file__), IDX_SUFFIX + '_test_criteria.ini')
TEST_DATA_DIR = os.path.dirname(criteria.__file__) + '/tests/data'
INI_CONFIG = None


@override_settings(ELASTIC=PydginTestSettings.OVERRIDE_SETTINGS)
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

    PydginTestSettings.setupIdx(['DISEASE', 'MARKER', 'GENE_CRITERIA_IS_GENE_IN_MHC',
                                 'GENE_CRITERIA_CAND_GENE_IN_STUDY',
                                 'GENE_CRITERIA_GENE_IN_REGION', 'GENE_CRITERIA_CAND_GENE_IN_REGION'])

    # create the gene index
    # call_command('criteria_index', '--feature', 'gene', '--test')
    # Search.index_refresh(INI_CONFIG['DEFAULT']['CRITERIA_IDX_GENE'])


@override_settings(ELASTIC=PydginTestSettings.OVERRIDE_SETTINGS)
def tearDownModule():
    # remove index created
    global INI_CONFIG
    os.remove(MY_INI_FILE)
    # requests.delete(ElasticSettings.url() + '/' + INI_CONFIG['DEFAULT']['CRITERIA_IDX_GENE'])

    PydginTestSettings.tearDownIdx(['DISEASE', 'GENE_CRITERIA_IS_GENE_IN_MHC', 'GENE_CRITERIA_CAND_GENE_IN_STUDY',
                                    'GENE_CRITERIA_GENE_IN_REGION', 'GENE_CRITERIA_CAND_GENE_IN_REGION'])


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

        self.region_doc_17q = {
            '_index': "regions_v0.0.5",
            '_type': "region",
            '_id': "17q21.2_007",
            '_score': 7.7001157,
            '_source': {
                'tier': 1,
                'seqid': "17",
                'disease_loci': [
                    "MS_17002",
                    "CRO_17004",
                    "IBD_17003",
                    "UC_17002",
                    "PSO_17002"
                    ],
                'region_name': "17q21.2",
                'tags': {
                         'disease': [
                            "UC",
                            "PSO",
                            "IBD",
                            "MS",
                            "CRO"
                                ],
                         'weight': 231
                },
                'species': "Human",
                'region_id': "17q21.2_007"
            }
                               }

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

        self.region_hit = {
            '_index': "regions_v0.0.5",
            '_type': "hits",
            '_id': "AVLFmnd7GA5k1HUlJV9R",
            '_source': {
                        'disease_locus': "1p13.2",
                        'status': "N",
                        'disease': "RA",
                        'marker': "rs2476601",
                        'dil_study_id': "GDXHsS00019",
            }
            }

    def test_gene_in_region(self):
        ''' Test process_gene_in_region. '''
        config = IniParser().read_ini(MY_INI_FILE)

        # pass a region document
        criteria_results = GeneCriteria.gene_in_region(self.region_doc_full, config=config, result_container={})
        expected_dict = {'ENSG00000279625': {'IBD': [{'fid': '1p36.12_008', 'fname': '1p36.12'}],
                                             'UC': [{'fid': '1p36.12_008', 'fname': '1p36.12'}]}}
        self.assertEqual(criteria_results, expected_dict, 'Got regions in gene as expected')

        criteria_results_17q = GeneCriteria.gene_in_region(self.region_doc_17q, config=config,
                                                           result_container={})
        self.assertTrue(len(criteria_results_17q) > 20, "Got back results greater than the default size")

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

    def test_exonic_index_snp_in_gene(self):
        ''' Test exonic_index_snp_in_gene. '''
        config = IniParser().read_ini(MY_INI_FILE)

        # pass a region document
        criteria_results = GeneCriteria.exonic_index_snp_in_gene(self.region_hit,
                                                                 config=config, result_container={})

        expected_result = {'ENSG00000226167': {'RA': [{'fid': 'rs2476601',
                                                       'fnotes': {'linkname': 'Eyre S', 'linkid': 'GDXHsS00019'},
                                                       'fname': 'rs2476601'}]},
                           'ENSG00000134242': {'RA': [{'fid': 'rs2476601',
                                                       'fnotes': {'linkname': 'Eyre S', 'linkid': 'GDXHsS00019'},
                                                       'fname': 'rs2476601'}]}}
        self.assertEqual(criteria_results, expected_result, 'Got back expected result')

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

    @override_settings(ELASTIC=PydginTestSettings.OVERRIDE_SETTINGS)
    def test_get_disease_tags(self):

        # feature_id = self.get_random_feature_id()
        feature_id = 'ENSG00000134242'
        disease_docs = GeneCriteria.get_disease_tags(feature_id)

        disease_tags = [getattr(disease_doc, 'code') for disease_doc in disease_docs]
        self.assertTrue(len(disease_tags) > 0, 'disease_tags present')

    def test_available_criterias(self):
        config = IniParser().read_ini(MY_INI_FILE)
        available_criterias = GeneCriteria.get_available_criterias(config=config)
        expected_dict = {'gene': ['cand_gene_in_study', 'gene_in_region', 'is_gene_in_mhc', 'cand_gene_in_region']}
        self.assertIsNotNone(available_criterias, 'Criterias as not none')
        self.assertIn('cand_gene_in_study', available_criterias['gene'])
        self.assertEqual(available_criterias.keys(), expected_dict.keys(), 'Dic keys equal')

    @override_settings(ELASTIC=PydginTestSettings.OVERRIDE_SETTINGS)
    def test_get_criteria_details(self):
        config = IniParser().read_ini(MY_INI_FILE)
        idx = ElasticSettings.idx('GENE_CRITERIA')
        available_criterias = GeneCriteria.get_available_criterias(config=config)['gene']
        idx_type = ','.join(available_criterias)
        doc_by_idx_type = ElasticUtils.get_rdm_docs(idx, idx_type, size=1)
        self.assertTrue(len(doc_by_idx_type) > 0)
        feature_id = getattr(doc_by_idx_type[0], 'qid')

        criteria_details = GeneCriteria.get_criteria_details(feature_id, config=config)

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

    @override_settings(ELASTIC=PydginTestSettings.OVERRIDE_SETTINGS)
    def test_get_disease_tags_from_results(self):
        config = IniParser().read_ini(MY_INI_FILE)
        feature_id = self.get_random_feature_id()

        criteria_details = GeneCriteria.get_criteria_details(feature_id, config=config)
        dis_codes = GeneCriteria.get_disease_codes_from_results(criteria_details)
        self.assertTrue(len(dis_codes) > 0, 'Got disease codes')

    @override_settings(ELASTIC=PydginTestSettings.OVERRIDE_SETTINGS)
    def test_get_disease_tags_as_codes(self):

        feature_id = 'ENSG00000134242'
        disease_docs = GeneCriteria.get_disease_tags(feature_id)
        disease_codes = GeneCriteria.get_disease_tags_as_codes(feature_id)
        self.assertEqual(len(disease_docs), len(disease_codes), 'Got the same disease code size')

        print(len(disease_docs))
        print(len(disease_codes))

        doc1 = disease_docs[0]
        dis1 = disease_codes[0]

        self.assertTrue(isinstance(doc1, disease.document.DiseaseDocument), 'Got the disease doc')
        self.assertTrue(isinstance(dis1, str), 'Got the disease code')

    def get_random_feature_id(self):
        config = IniParser().read_ini(MY_INI_FILE)
        idx = ElasticSettings.idx('GENE_CRITERIA')
        available_criterias = GeneCriteria.get_available_criterias(config=config)['gene']
        idx_type = ','.join(available_criterias)
        doc_by_idx_type = ElasticUtils.get_rdm_docs(idx, idx_type, size=1)
        self.assertTrue(len(doc_by_idx_type) > 0)
        feature_id = getattr(doc_by_idx_type[0], 'qid')
        return feature_id

    @override_settings(ELASTIC=PydginTestSettings.OVERRIDE_SETTINGS)
    def test_get_all_criteria_disease_tags(self):

        feature_id1 = 'ENSG00000134242'
        feature_id2 = 'ENSG00000227609'
        criteria_disease_tags = GeneCriteria.get_all_criteria_disease_tags([feature_id1,
                                                                            feature_id2])

        self.assertIn(feature_id1, criteria_disease_tags)
        self.assertIn(feature_id2, criteria_disease_tags)
        self.assertIn('all', criteria_disease_tags[feature_id1])
        self.assertIn('all', criteria_disease_tags[feature_id2])
