import logging
from data_pipeline.utils import IniParser
import os
from builtins import classmethod
from disease import utils
import datetime

# Get an instance of a logger
logger = logging.getLogger(__name__)


class CriteriaManager():
    '''CriteriaManager defined functions some utility functions common to all criterias
    '''

    @classmethod
    def get_criteria_config(cls):
        '''function to build the criteria config
        '''
        BASE_DIR = os.path.dirname(os.path.dirname(__file__))
        ini_file = os.path.join(BASE_DIR, 'criteria.ini')
        config = None
        if os.path.isfile(ini_file):
            config = IniParser.read_ini(cls, ini_file=ini_file)

        return config

    @classmethod
    def get_available_diseases(cls, tier=None):
        '''function to get the disease codes enabled in site
        '''
        (main_codes, other_codes) = utils.Disease.get_site_disease_codes()

        if tier == 0:
            return main_codes
        elif tier == 1:
            return other_codes
        else:
            return (main_codes, other_codes)

    @classmethod
    def process_criterias(cls, feature, criteria=None, config=None, show=False):
        '''function to delegate the call to the right criteria class and build the criteria for that class
        '''
        from criteria.helper.criteria import Criteria
        from criteria.helper.gene_criteria import GeneCriteria
        from criteria.helper.marker_criteria import MarkerCriteria
        from criteria.helper.region_criteria import RegionCriteria
        from criteria.helper.study_criteria import StudyCriteria

        if config is None:
            config = cls.get_criteria_config()

        available_criterias = Criteria.get_available_criterias(feature)[feature]

        criterias_to_process = []
        if criteria is None:
            criterias_to_process = available_criterias
        else:
            criterias_list = criteria.split(',')
            criterias_to_process = [criteria.strip() for criteria in criterias_list
                                    if criteria.strip() in available_criterias]

        if show:
            logger.debug(criterias_to_process)
            print(criterias_to_process)
            return criterias_to_process

        logger.debug(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
        for section in criterias_to_process:
            if feature == 'gene':
                print('Call to build criteria gene index')
                Criteria.process_criteria(feature, section, config, GeneCriteria)
            elif feature == 'marker':
                print('Call to build criteria marker index')
                Criteria.process_criteria(feature, section, config, MarkerCriteria)
            elif feature == 'region':
                print('Call to build criteria region index')
                Criteria.process_criteria(feature, section, config, RegionCriteria)
            elif feature == 'study':
                print('Call to build criteria study index')
                Criteria.process_criteria(feature, section, config, StudyCriteria)
            else:
                logger.critical('Unsupported feature ... please check the inputs')

        logger.debug(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
        logger.debug('========DONE==========')
