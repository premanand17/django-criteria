''' Define a resource for criteria data to be used in Django REST framework. '''
from django.http.response import Http404
from rest_framework.filters import DjangoFilterBackend, OrderingFilter
from rest_framework.response import Response
from criteria.helper.gene_criteria import GeneCriteria
from criteria.helper.marker_criteria import MarkerCriteria
from criteria.helper.region_criteria import RegionCriteria
from criteria.helper.study_criteria import StudyCriteria
import re
from criteria.helper.criteria import Criteria
from elastic.rest_framework.resources import ListElasticMixin,\
    ElasticFilterBackend, RetrieveElasticMixin
from elastic.search import ElasticQuery, Search
from elastic.query import Query
from elastic.rest_framework.elastic_obj import ElasticObject
from elastic.elastic_settings import ElasticSettings


# class CriteriaFilterBackend(OrderingFilter, DjangoFilterBackend):
# 
#     FEATURE_TYPE_LIST = ['GENE', 'MARKER', 'REGION', 'STUDY', 'ALL']
# 
#     def filter_queryset(self, request, queryset, view):
#         ''' Override this method to request feature locations. '''
#         try:
#             filterable = getattr(view, 'filter_fields', [])
#             filters = dict([(k, v) for k, v in request.GET.items() if k in filterable])
#             feature_type_str = filters.get('feature_type', 'GENE')
#             feature_id = filters.get('feature_id')
# 
#             identifiers_list = []
#             identifiers_list = re.split('\n|,', feature_id)
#             identifiers = [identifier.rstrip() for identifier in identifiers_list]
# 
#             print('=================')
#             print(feature_type_str)
#             print(identifiers)
#             print(request.data)
#             print('=================')
# 
#             criteria_details = None
#             if feature_type_str == 'GENE':
#                 criteria_details = GeneCriteria.get_all_criteria_disease_tags(identifiers)
#             elif feature_type_str == 'MARKER':
#                 criteria_details = MarkerCriteria.get_all_criteria_disease_tags(identifiers)
#             elif feature_type_str == 'REGION':
#                 criteria_details = RegionCriteria.get_all_criteria_disease_tags(identifiers)
#             elif feature_type_str == 'STUDY':
#                 criteria_details = StudyCriteria.get_all_criteria_disease_tags(identifiers)
#             elif feature_type_str == 'ALL':
#                 criteria_details = Criteria.do_criteria_search(identifiers)
# 
#             criterias = []
#             disease_tags = []
#             if feature_type_str == 'ALL':
#                 for feature_type, feature_result in criteria_details.items():
#                     for feature_id, feature_value in feature_result.items():
#                         if 'all' in feature_value:
#                             disease_tags = feature_value['all']
#                             criterias.append({'feature_type': feature_type.upper(),
#                                               'feature_id': feature_id,
#                                               'disease_tags': disease_tags,
#                                               'feature_name': feature_id})
#             else:
#                 for feature_id, feature_value in criteria_details.items():
#                     print(feature_id)
#                     if 'all' in feature_value:
#                         disease_tags = feature_value['all']
#                     criterias.append({'feature_type': feature_type_str,
#                                       'feature_id': feature_id,
#                                       'disease_tags': disease_tags,
#                                       'feature_name': feature_id})
# 
#             return criterias
#         except (TypeError, ValueError, IndexError, ConnectionError):
#             raise Http404


class CriteriaFilterBackend(ElasticFilterBackend):

    FEATURE_TYPE_MAP = {
        'GENE': 'GENE_CRITERIA',
        'MARKER': 'MARKER_CRITERIA',
        'REGION': 'REGION_CRITERIA',
        'STUDY': 'STUDY_CRITERIA',
        'ALL': ['GENE_CRITERIA', 'MARKER_CRITERIA', 'REGION_CRITERIA', 'STUDY_CRITERIA']
    }

    def _get_index(self, ftype):
        ''' Given the build return the build number as an integer. '''
        for f, idx in CriteriaFilterBackend.FEATURE_TYPE_MAP.items():
            if f == ftype:
                return idx

        return CriteriaFilterBackend.FEATURE_TYPE_MAP['GENE']

    def filter_queryset(self, request, queryset, view):
        ''' Override this method to request just the documents required from elastic. '''
        q_size = view.paginator.get_limit(request)
        q_from = view.paginator.get_offset(request)

        filterable = getattr(view, 'filter_fields', [])
        print(filterable)
        print(request)
        filters = dict([(k, v) for k, v in request.GET.items() if k in filterable])
        criteria_idx = self._get_index(filters.get('feature_type', 'GENE_CRITERIA'))

        idx = criteria_idx
        if type(criteria_idx) == list:
            idx = ','.join(ElasticSettings.idx(name) for name in criteria_idx)
        else:
            idx = ElasticSettings.idx(criteria_idx)

        q = ElasticQuery(Query.match_all())
        s = Search(search_query=q, idx=idx, size=q_size, search_from=q_from)
        json_results = s.get_json_response()
        results = []
        for result in json_results['hits']['hits']:
            new_obj = ElasticObject(initial=result['_source'])
            new_obj.uuid = result['_id']
            new_obj.criteria_type = result['_type']
            results.append(new_obj)
        view.es_count = json_results['hits']['total']
        return results


class ListCriteriaMixin(ListElasticMixin):
    ''' Get a list of criterias for a feature. '''
    filter_backends = [CriteriaFilterBackend, ]


class RetrieveCriteriaMixin(RetrieveElasticMixin):

    def get_object(self):
        q = ElasticQuery(Query.ids(self.kwargs[self.lookup_field]))
        s = Search(search_query=q, idx=getattr(self, 'idx'))
        try:
            result = s.get_json_response()['hits']['hits'][0]
            obj = ElasticObject(initial=result['_source'])
            obj.uuid = result['_id']
            obj.criteria_type = result['_type']

            # May raise a permission denied
            self.check_object_permissions(self.request, obj)
            return obj
        except (TypeError, ValueError, IndexError):
            raise Http404
