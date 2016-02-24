''' Command line tool to manage downloads. '''
from django.core.management.base import BaseCommand
from criteria.helper.criteria_manager import CriteriaManager


class Command(BaseCommand):
    '''
    Command lines for loading criteria data.
    Criteria meta info:
    ./manage.py criteria_index --feature gene --criteria cand_gene_in_study
    ./manage.py criteria_index --feature gene --test
    ./manage.py criteria_index --feature marker --criteria is_in_mhc
    '''
    help = "Create criteria indexes(s)."

    def add_arguments(self, parser):
        parser.add_argument('--feature',
                            dest='feature',
                            help='Feature/Object Type defining criteria (eg: gene, marker, region)', required=True)
        parser.add_argument('--criteria',
                            dest='criteria',
                            help='Comma separated criteria names (e.g. cand_gene_in_study) to build indexes [default: all].')  # @IgnorePep8
        parser.add_argument('--show',
                            dest='show',
                            action='store_true',
                            help='List all criterias')
        parser.add_argument('--test',
                            dest='test',
                            action='store_true',
                            help='Run in test mode')

    def handle(self, *args, **options):
        criteria_manager = CriteriaManager()
        feature_ = None
        criteria_ = None
        if 'feature' in options:
            feature_ = options['feature']
        if 'criteria' in options:
            criteria_ = options['criteria']
        if 'show' in options:
            show_ = options['show']
        if 'test' in options:
            test_ = options['test']

        if test_:
            config_ = criteria_manager.get_criteria_config(ini_file='test_criteria.ini')
        else:
            config_ = criteria_manager.get_criteria_config(ini_file='criteria.ini')

        criteria_manager.process_criterias(feature=feature_, criteria=criteria_, config=config_, show=show_, test=test_)
