import logging
from data_pipeline.utils import IniParser
import os
from builtins import classmethod
import sys
from disease import utils

# Get an instance of a logger
logger = logging.getLogger(__name__)


class CriteriaManager():

    @classmethod
    def get_criteria_config(cls):
        BASE_DIR = os.path.dirname(os.path.dirname(__file__))
        ini_file = os.path.join(BASE_DIR, 'criteria.ini')
        config = None
        if os.path.isfile(ini_file):
            config = IniParser.read_ini(cls, ini_file=ini_file)

        return config

    @classmethod
    def get_available_diseases(cls, tier=None):
        # Get it from elastic later
        (main_codes, other_codes) = utils.Disease.get_site_disease_codes()

        if tier == 0:
            return main_codes
        elif tier == 1:
            return other_codes
        else:
            return (main_codes, other_codes)

    @classmethod
    def get_available_criterias(cls, feature=None, config=None):

        if config is None:
            config = cls.get_criteria_config()

        criteria_dict = dict()
        criteria_list = []
        for section_name in config.sections():
            if config[section_name] is not None:
                section_config = config[section_name]
                if 'feature' in section_config:
                    if feature is not None and feature != section_config['feature']:
                        continue

                    if section_config['feature'] in criteria_dict:
                        criteria_list = criteria_dict[section_config['feature']]
                        criteria_list.append(section_name)
                    else:
                        criteria_list = []
                        criteria_list.append(section_name)
                        criteria_dict[section_config['feature']] = criteria_list

        return criteria_dict

    @classmethod
    def process_criterias(cls, feature, criteria=None, config=None, show=False):

        from criteria.helper.gene_criteria import GeneCriteria

        if config is None:
            config = cls.get_criteria_config()

        available_criterias = cls.get_available_criterias(feature)[feature]

        criterias_to_process = []
        if criteria is None:
            criterias_to_process = available_criterias
        else:
            criterias_list = criteria.split(',')
            criterias_to_process = [criteria.strip() for criteria in criterias_list
                                    if criteria.strip() in available_criterias]

        if show:
            return criterias_to_process

        for section in criterias_to_process:
            if feature == 'gene':
                print('Call to build criteria gene index')
                GeneCriteria.process_gene_criteria(feature, section, config)
            elif feature == 'marker':
                print('Call to build criteria marker index')
            elif feature == 'region':
                print('Call to build criteria region index')
            elif feature == 'study':
                print('Call to build criteria study index')
            else:
                logger.critical('Unsupported feature ... please check the inputs')
