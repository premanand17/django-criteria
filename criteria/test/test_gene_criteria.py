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
                     'pmids': ['12477932', '12975309', '15489334', '17004324', '21103663', '26186194']}

        self.gene_src_position1 = {'_id': 'ENSG00000136634', 'start': 206767602, 'stop': 206772494, 'chromosome': '1'}  # IL10
        self.gene_ensembl1 = {'_id': 'ENSG00000136634'}

    def test_process_gene_in_region(self):
        ''' Test process_gene_in_region. '''
        config = IniParser().read_ini(MY_INI_FILE)
        section = config["gene_in_region"]
        criteria_details = GeneCriteria.gene_in_region(self.gene_ensembl1['_id'], section, config)
        self.assertIn('disease_tags', criteria_details, 'disease_tags in container')
        self.assertIn('score', criteria_details, 'score in container')
        self.assertIn('SLE', criteria_details, 'score in container')

    def test_cand_gene_in_study(self):
        config = IniParser().read_ini(MY_INI_FILE)

        input_doc = {'_source': {'diseases': ['T1D', 'MS'],
                     'genes': ['ENSG00000110848', 'ENSG00000160791', 'ENSG00000163599'],
                     'study_id': 'GDXHsS00004', 'authors': ['Barrett', 'Type 1 Diabetes Genetics Consortium']},
                     '_type': 'studies',
                     '_index': 'studies_latest', '_id': 'GDXHsS00004', '_score': 0.0}

        expected_doc = {'ENSG00000110848': {'T1D': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}],
                                            'MS': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}]},
                        'ENSG00000160791': {'T1D': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}],
                                            'MS': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}]},
                        'ENSG00000163599': {'T1D': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}],
                                            'MS': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}]}}
        result_doc = GeneCriteria.cand_gene_in_study(input_doc['_source'], config=config)
        self.assertEqual(expected_doc, result_doc, 'dicts are equal and as expected')

        input_doc = {'_source': {'diseases': ['RA', 'T1D'],
                     'genes': ['ENSG00000110800', 'ENSG00000160801', 'ENSG00000163599'],
                     'study_id': 'GDXHsS00005', 'authors': ['Clatfield', 'Type 1 Diabetes Genetics Consortium']},
                     '_type': 'studies',
                     '_index': 'studies_latest', '_id': 'GDXHsS00005', '_score': 0.0}

        expected_doc = {'ENSG00000160801': {'RA': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield'}],
                                            'T1D': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield'}]},
                        'ENSG00000163599': {'RA': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield'}],
                                            'MS': [{'fid': 'GDXHsS00004', 'fname': 'Barrett'}],
                                            'T1D': [{'fid': 'GDXHsS00004', 'fname': 'Barrett'},
                                                    {'fid': 'GDXHsS00005', 'fname': 'Clatfield'}]},
                        'ENSG00000160791': {'T1D': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}],
                                            'MS': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}]},
                        'ENSG00000110800': {'RA': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield'}],
                                            'T1D': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield'}]},
                        'ENSG00000110848': {'T1D': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}],
                                            'MS': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}]}}

        updated_doc = GeneCriteria.cand_gene_in_study(input_doc['_source'], config=config, result_container=result_doc)
        self.assertEqual(expected_doc, updated_doc, 'dicts are equal and as expected')

        input_doc = {'_source': {'diseases': ['AA'],
                     'genes': ['ENSG00000110900'],
                     'study_id': 'GDXHsS00006', 'authors': ['AaTestAuthor', 'Type 1 Diabetes Genetics Consortium']},
                     '_type': 'studies',
                     '_index': 'studies_latest', '_id': 'GDXHsS00006', '_score': 0.0}

        expected_doc = {'ENSG00000160801': {'RA': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield'}],
                                            'T1D': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield'}]},
                        'ENSG00000163599': {'RA': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield'}],
                                            'MS': [{'fid': 'GDXHsS00004', 'fname': 'Barrett'}],
                                            'T1D': [{'fid': 'GDXHsS00004', 'fname': 'Barrett'},
                                                    {'fid': 'GDXHsS00005', 'fname': 'Clatfield'}]},
                        'ENSG00000160791': {'T1D': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}],
                                            'MS': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}]},
                        'ENSG00000110800': {'RA': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield'}],
                                            'T1D': [{'fid': 'GDXHsS00005', 'fname': 'Clatfield'}]},
                        'ENSG00000110900': {'AA': [{'fid': 'GDXHsS00006', 'fname': 'AaTestAuthor'}]},
                        'ENSG00000110848': {'T1D': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}],
                                            'MS': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}]}}

        updated_doc = GeneCriteria.cand_gene_in_study(input_doc['_source'], config=config, result_container=result_doc)
        self.assertEqual(expected_doc, updated_doc, 'dicts are equal and as expected')

    def test_get_criteria_dict(self):

        expected_dict = {'fid': 'GDXHsS00004', 'fname': 'Barrett'}
        criteria_dict = GeneCriteria.get_criteria_dict('GDXHsS00004', 'Barrett')
        self.assertEqual(expected_dict, criteria_dict, 'dicts are equal')

        expected_dict = {'fid': 'GDXHsS00004', 'fnotes': {'rsq': '0.1'}, 'fname': 'Barrett'}
        criteria_dict = GeneCriteria.get_criteria_dict('GDXHsS00004', 'Barrett', {'rsq': '0.1'})
        self.assertEqual(expected_dict, criteria_dict, 'dicts are equal')

    def test_get_criteria_disease_dict(self):
        criteria_dict = GeneCriteria.get_criteria_dict('GDXHsS00004', 'Barrett')
        diseases = ['T1D']
        criteria_disease_dict = GeneCriteria.get_criteria_disease_dict(diseases, criteria_dict, {})
        expected_dict = {'T1D': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}]}
        self.assertEqual(criteria_disease_dict, expected_dict, 'Dict as expected')

        criteria_disease_dict = GeneCriteria.get_criteria_disease_dict(diseases, criteria_dict, criteria_disease_dict)
        self.assertEqual(criteria_disease_dict, expected_dict, 'Dict as expected after addding duplicate')

        criteria_dict = GeneCriteria.get_criteria_dict('GDXHsS00005', 'Catfield')
        expected_dict = {'T1D': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'},
                                 {'fname': 'Catfield', 'fid': 'GDXHsS00005'}]}
        criteria_disease_dict = GeneCriteria.get_criteria_disease_dict(diseases, criteria_dict, criteria_disease_dict)
        self.assertEqual(criteria_disease_dict, expected_dict, 'Dict as expected after adding new')

        diseases = ['T1D', 'MS']
        criteria_disease_dict = GeneCriteria.get_criteria_disease_dict(diseases, criteria_dict, criteria_disease_dict)
        expected_dict = {'MS': [{'fname': 'Catfield', 'fid': 'GDXHsS00005'}],
                         'T1D': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'},
                                 {'fname': 'Catfield', 'fid': 'GDXHsS00005'}]}
        self.assertEqual(criteria_disease_dict, expected_dict, 'Dict as expected after adding diseases')

    def test_process_cand_gene_in_study(self):
        ''' Test process_gene_in_region. '''
        config = IniParser().read_ini(MY_INI_FILE)
        disease_list = GeneCriteria.cand_gene_in_study(self.gene_ensembl1, config, details=False)
        self.assertEqual(len(disease_list), 5, 'Got 5 diseases')
        self.assertEqual({'CRO', 'IBD', 'SLE', 'UC', 'T1D'}, set(disease_list),
                         'disease_list are equal ' + str(disease_list))

        criteria_details = GeneCriteria.cand_gene_in_study(self.gene_ensembl1, config, details=True)
        self.assertIn('disease_tags', criteria_details, 'disease_tags in container')
        self.assertIn('score', criteria_details, 'score in container')
        self.assertIn('SLE', criteria_details, 'score in container')

    def test_calculate_score(self):
        disease_list = ["AA", "T1D"]
        score = GeneCriteria.calculate_score(disease_list)
        self.assertEqual(score, 15, "Got back the right score")

    def test_get_available_criterias(self):

        INI_CONFIG = IniParser().read_ini(MY_INI_FILE)

        feature_id = "ENSG00000111252"
        default_section = INI_CONFIG['DEFAULT']
        criteria_index = default_section['CRITERIA_IDX']
        available_criterias = GeneCriteria.get_available_criterias(feature_id, criteria_index)
        print(available_criterias)

    def test_format_disease_tags(self):

        disease_tags = [{'criteria': ['cand_gene_in_study', 'gene_in_region'], 'disease': 'RA'},
                        {'criteria': ['cand_gene_in_study'], 'disease': 'CEL'}]

        expected_disease_tags = {'RA': ['cand_gene_in_study', 'gene_in_region'],
                                 'CEL': ['cand_gene_in_study']}
        formattted_disease_tags = GeneCriteria.format_disease_tags(disease_tags)
        self.assertEqual(expected_disease_tags, formattted_disease_tags, 'got back the right format')
        print(formattted_disease_tags)

    def test_tag_feature_to_disease(self):
        ''' Test tag_feature_to_disease. '''
        config = IniParser().read_ini(MY_INI_FILE)
        section = config["gene_in_region"]
        GeneCriteria().tag_feature_to_disease('GeneCriteria', self.gene_src_position1, section, config)

        section = config["cand_gene_in_study"]
        GeneCriteria().tag_feature_to_disease('GeneCriteria', self.gene_src_position1, section, config)
