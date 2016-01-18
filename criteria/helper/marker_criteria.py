import logging
from builtins import classmethod
from elastic.search import ScanAndScroll, ElasticQuery, Search
from elastic.query import Query, BoolQuery, RangeQuery
from data_pipeline.helper import marker

logger = logging.getLogger(__name__)


class MarkerCriteria():

    ''' MarkerCriteria class define functions for building marker index type within criteria index

    '''
    global result_container
    result_container = {}

    @classmethod
    def process_marker_criteria(cls, stage_output_file, *args, **kwargs):
        print('=========process_marker_criteria called=================')

        print('stage_output_file : ' + str(stage_output_file))
        section = kwargs['section']
        config = None
        if 'config' in kwargs:
            config = kwargs['config']

        default_section = config['DEFAULT']
        region_index = default_section['REGION_IDX']

        def process_hits(resp_json):
            hits = resp_json['hits']['hits']
            global result_container
            marker_container = set()

            for hit in hits:
                marker_container.add(hit['_source']['marker'])

            for marker_id in marker_container:
                disease_ids = cls.tag_feature_to_disease(marker_id, section, config)
                if (len(disease_ids) > 0):
                    print(marker_id + ' ' + str(section._name) + ' ' + str(disease_ids))

                    if marker_id not in result_container:
                        result_container[marker_id] = [{str(section._name): list(disease_ids)}]
                    else:
                        existing_criteria = result_container[marker_id]
                        existing_criteria.append({str(section._name): list(disease_ids)})
                        result_container[marker_id] = existing_criteria

        qbool = BoolQuery(must_arr=[RangeQuery("tier", lt=3)])
        query = ElasticQuery.filtered_bool(Query.match_all(), qbool)
        ScanAndScroll.scan_and_scroll(region_index, call_fun=process_hits, idx_type='hits', query=query)
        # ScanAndScroll.scan_and_scroll(region_index, call_fun=process_hits)
        # CriteriaUtils.create_json_output_criteria(result_container, stage_output_file)

    @classmethod
    def is_an_index_snp(cls, feature_src, section, config=None):
        '''Function to process the criteria cand_gene_in_study'''
        region_index = config['DEFAULT']['REGION_IDX']

        if type(feature_src) == dict:
            feature_id = str(feature_src['_id'])
        else:
            feature_id = feature_src

        qbool = BoolQuery(must_arr=[RangeQuery("tier", lt=3),
                                    Query.term("marker", feature_id), Query.term("status", "n")])
        query = ElasticQuery.filtered_bool(Query.match_all(), qbool, sources=['disease', 'tier', 'status', 'marker'])
        elastic = Search(query, idx=region_index)
        result = elastic.search()

        disease_list = set()
        for doc in result.docs:
            tier = getattr(doc, 'tier')
            status = getattr(doc, 'status')
            marker = getattr(doc, 'marker')
            disease = getattr(doc, 'disease')

            if tier < 3 and status is 'N' and marker == feature_id:
                disease_list.add(disease)

        return disease_list
