from django.test import TestCase
from elastic.elastic_settings import ElasticSettings
import os
import criteria
from data_pipeline.utils import IniParser
from criteria.helper.criteria import Criteria

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


class CriteriaTest(TestCase):
    '''Test interaction staging'''

    def test_is_in_mhc(self):
            ''' Test is in mhc '''
            config = IniParser().read_ini(MY_INI_FILE)
            section = "is_in_mhc"

            result_docs = Criteria.is_in_mhc('gene', seqid_param='chromosome', start_param='start', end_param='stop', section=section, config=config)
            #self.assertTrue(len(result_docs) > 100, 'No of docs returned ' + str(len(result_docs)))
            #print('No of docs returned ' + str(len(result_docs)))

            #result_docs = Criteria.is_in_mhc('marker', seqid_param='seqid', start_param='start', end_param='end', section=section, config=config)
            #self.assertTrue(len(result_docs) > 100, 'No of docs returned ' + str(len(result_docs)))
            #print('No of docs returned ' + str(len(result_docs)))
            
    def test_tag_feature_to_all_diseases(self):
        config = IniParser().read_ini(MY_INI_FILE)
        section = "is_in_mhc"
        feature_id = 'ENSG00000229281'
        result = Criteria.tag_feature_to_all_diseases(feature_id, section, config, {})
        print(result)
        
    
