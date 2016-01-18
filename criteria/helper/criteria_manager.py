import logging
from data_pipeline.utils import IniParser
import os
from builtins import classmethod

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
        tier1 = ['AS', 'ATD', 'CEL', 'CRO', 'JIA', 'MS', 'PBC', 'PSO', 'RA', 'SLE', 'T1D', 'UC']
        tier2 = ['AA', 'IGE', 'IBD', 'NAR', 'PSC', 'SJO', 'SSC', 'VIT']

        if tier == 1:
            return tier1
        elif tier == 2:
            return tier2
        else:
            return tier1 + tier2

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
    def process_criterias(cls, feature, criteria=None, config=None):

        if config is None:
            config = cls.get_criteria_config()

        available_criterias = cls.get_available_criterias(feature)[feature]
        print(available_criterias)

        criterias_to_process = []
        if criteria is None:
            criterias_to_process = available_criterias
        else:
            criterias_list = criteria.split(',')
            print(criterias_list)
            criterias_to_process = [criteria.strip() for criteria in criterias_list
                                    if criteria.strip() in available_criterias]

        print(criterias_to_process)

        for section in criterias_to_process:
            if feature == 'gene':
                print('Call to build criteria gene index')
                from criteria.helper.gene_criteria import GeneCriteria
                GeneCriteria.process_gene_criteria(section, config)
            elif feature == 'marker':
                print('Call to build criteria marker index')
            elif feature == 'region':
                print('Call to build criteria region index')
            elif feature == 'study':
                print('Call to build criteria study index')
            else:
                logger.critical('Unsupported feature ... please check the inputs')
