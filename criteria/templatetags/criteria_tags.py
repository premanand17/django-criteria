''' Template tags for the criteria app. '''
from django import template
from criteria.helper.criteria import Criteria
from criteria.helper.gene_criteria import GeneCriteria  # @UnusedImport
from criteria.helper.marker_criteria import MarkerCriteria  # @UnusedImport
from criteria.helper.region_criteria import RegionCriteria  # @UnusedImport
from criteria.helper.study_criteria import StudyCriteria  # @UnusedImport

register = template.Library()


@register.inclusion_tag('sections/criteria.html')
def show_feature_criteria_details(feature_id, feature_type, feature_doc=None, section='criteria',
                                  section_title="criteria"):
    ''' Template inclusion tag to render criteria details bar. '''
    print('===================')
    print(feature_id)
    print(feature_type)
    print('====================')
    (idx, idx_types) = Criteria.get_feature_idx_n_idxtypes(feature_type)
    criteria_disease_tags = Criteria.get_all_criteria_disease_tags([feature_id], idx, idx_types)
    print(criteria_disease_tags)
    return {'criteria': criteria_disease_tags, 'feature_id': feature_id, 'appname': feature_type,
            'f': feature_doc, 'section': section, 'section_title': section_title}
