''' Test for criteria rest interface. '''
import json

from django.core.urlresolvers import reverse
from elastic.elastic_settings import ElasticSettings
import os
import criteria
from data_pipeline.utils import IniParser
from pydgin.tests.data.settings_idx import PydginTestSettings
from django.test.testcases import TestCase
from django.test.utils import override_settings


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


@override_settings(ELASTIC=PydginTestSettings.OVERRIDE_SETTINGS)
class CriteraRestTest(TestCase):

    def test_gene_list(self):
        ''' Test retrieving criteria regions. '''
        url = reverse('rest:criteria-list')
        response = self.client.get(url, data={'feature_type': 'GENE'})
        criteria = json.loads(response.content.decode("utf-8"))
        self.assertGreater(len(criteria), 0, 'results found')
