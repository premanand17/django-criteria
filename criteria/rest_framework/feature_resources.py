''' Define a resource for criteria data to be used in Django REST framework. '''
from django.http.response import Http404
from criteria.helper.criteria import Criteria
from elastic.rest_framework.resources import ListElasticMixin,\
    ElasticFilterBackend, RetrieveElasticMixin
from elastic.search import ElasticQuery, Search
from elastic.query import Query
from elastic.rest_framework.elastic_obj import ElasticObject
from elastic.elastic_settings import ElasticSettings


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
        filters = dict([(k, v) for k, v in request.GET.items() if k in filterable])
        criteria_idx = self._get_index(filters.get('feature_type', 'GENE_CRITERIA'))

        feature_type = filters.get('feature_type')
        feature_id = filters.get('feature_id')
        aggregate = filters.get('aggregate')
        detail = filters.get('detail')

        idx = criteria_idx
        if type(criteria_idx) == list:
            idx = ','.join(ElasticSettings.idx(name) for name in criteria_idx)
        else:
            idx = ElasticSettings.idx(criteria_idx)

        results = []
        if feature_id and aggregate == 'true':
            disease_doc_tags = Criteria.get_disease_tags(feature_id, idx=idx)
            disease_tags = [getattr(d, 'code') for d in disease_doc_tags]
            new_obj = ElasticObject()
            new_obj.disease_tags = disease_tags
            new_obj.criteria_type = None
            results.append(new_obj)
            return results
        elif feature_id and detail == 'true':
            (idx, idx_types) = Criteria.get_feature_idx_n_idxtypes(feature_type.lower())
            criteria_details = Criteria.get_criteria_details(feature_id, idx=idx, idx_type=idx_types)
            criteria_list = idx_types.split(',')
            criteria_details_expanded = Criteria.add_meta_info(idx, criteria_list, criteria_details)

            feature_details = self._get_feature_details(criteria_details_expanded)

            for criteria, details in feature_details.items():
                print(criteria)
                new_obj = ElasticObject()
                new_obj.qid = details['qid']
                new_obj.criteria_type = criteria
                disease_tags = details['disease_tags']
                fdetails = list(details['fdetails'])
                print('+++++++++++++++')
                print(disease_tags)
                print(fdetails)
                print('+++++++++++++++')
                new_obj.disease_tags = disease_tags
                new_obj.feature_details = fdetails
                results.append(new_obj)

            return results
        else:
            q = ElasticQuery(Query.match_all())
            s = Search(search_query=q, idx=idx, size=q_size, search_from=q_from)
            json_results = s.get_json_response()

            for result in json_results['hits']['hits']:
                new_obj = ElasticObject(initial=result['_source'])
                new_obj.uuid = result['_id']
                new_obj.criteria_type = result['_type']
                results.append(new_obj)

            view.es_count = json_results['hits']['total']
            return results

    def _get_feature_details(self, criteria_details_expanded):

        feature_details = {}
        hits = criteria_details_expanded['hits']
        meta_info = criteria_details_expanded['meta_info']
        link_info = criteria_details_expanded['link_info']

        for hit in hits:
            _source = hit['_source']
            _index = hit['_index']
            _type = hit['_type']
            _id = hit['_id']
            _disease_tags = _source['disease_tags']
            _qid = _source['qid']
            link_id_type = link_info[_index][_type]
            _type_desc = meta_info[_index][_type]

            for dis in _disease_tags:
                fdetails = _source[dis]
                for fdetail in fdetails:
                    fid = fdetail['fid']
                    fname = fdetail['fname']

                    current_link = ""
                    current_row = ""
                    current_row += '<a href="/' + link_id_type + '/' + fid + '/">'
                    current_row += fname
                    current_row += '</a>'

                    print(current_row)

                    if 'fnotes' in fdetail:
                        fnotes = fdetail['fnotes']
                        print(fnotes)
                        link_data = ''
                        link_value = ''
                        link_id = ''
                        link_name = ''

                        if 'linkdata' in fnotes:
                            link_data = fnotes['linkdata']
                        if 'linkvalue' in fnotes:
                            link_value = fnotes['linkvalue']
                        if 'linkid' in fnotes:
                            link_id = fnotes['linkid']
                        if 'linkname' in fnotes:
                            link_name = fnotes['linkname']

                        if link_data and link_value:
                            current_row += ' ('+link_data+':'+str(link_value) + ')'

                        if link_id and link_name:
                            current_link += '<a href="/' + 'study' + '/' + link_id + '/">'
                            current_link += link_name
                            current_link += '</a>'

                if(len(current_link) > 0):
                    current_row += " - " + current_link

                print(current_row)

                if _type_desc in feature_details:
                    tmp_list = feature_details[_type_desc]['fdetails']
                    tmp_list.add(current_row)
                    feature_details[_type_desc]['fdetails'] = tmp_list
                    feature_details[_type_desc]['disease_tags'] = _disease_tags
                else:
                    tmp_list = set()
                    feature_details[_type_desc] = {}
                    tmp_list.add(current_row)
                    feature_details[_type_desc]['fdetails'] = tmp_list
                    feature_details[_type_desc]['disease_tags'] = _disease_tags

                # Add the qid
                feature_details[_type_desc]['qid'] = _qid

        return feature_details


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
