''' Command line tool to manage downloads. '''
from django.core.management.base import BaseCommand
from criteria.helper.criteria_manager import CriteriaManager


class Command(BaseCommand):
    '''
    Command lines for loading criteria data.
    Criteria meta info:
    ./manage.py criteria --ini criteria.ini --steps stage --feature meta_data --dir /tmp
    ./manage.py criteria --ini criteria.ini --steps stage --feature gene --dir /tmp

    ./manage.py criteria --ini criteria.ini --steps load --feature meta_data --dir /tmp
    ./manage.py criteria --ini criteria.ini --steps load --feature gene --dir /tmp
    '''
    help = "Stage/Load criteria indexes(s)."

    def add_arguments(self, parser):
        parser.add_argument('--feature',
                            dest='feature',
                            help='Feature/Object Type defining criteria (eg: gene, marker, region)', required=True)
        parser.add_argument('--criteria',
                            dest='criteria',
                            help='Comma separated criteria names (e.g. cand_gene_in_study) to build indexes [default: all].')  # @IgnorePep8

    def handle(self, *args, **options):
        criteria_manager = CriteriaManager()
        feature_ = None
        criteria_ = None
        if 'feature' in options:
            feature_ = options['feature']
        if 'criteria' in options:
            criteria_ = options['criteria']

        criteria_manager.process_criterias(feature=feature_, criteria=criteria_)
