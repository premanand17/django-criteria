from elastic.search import Search, ScanAndScroll, ElasticQuery
from elastic.elastic_settings import ElasticSettings
from elastic.query import BoolQuery, RangeQuery, OrFilter, Query
from criteria.helper.criteria_manager import CriteriaManager
from data_pipeline.utils import IniParser
from elastic.management.loaders.mapping import MappingProperties
from elastic.management.loaders.loader import Loader
from elastic.utils import ElasticUtils
import json

import logging
logger = logging.getLogger(__name__)

class Criteria(object):

#     @classmethod
#     def is_in_mhc(cls, feature, section=None, config=None):
#         print('+++++++++++++++++')
#         print(cls.__name__)
#         print('==================')
#         source_idx = None
#         source_idx_type = None
#         default_section = config['DEFAULT']
#         if feature == 'gene':
#             source_idx_type = 'GENE'
#             source_idx = ElasticSettings.idx('GENE', idx_type=source_idx_type)
#             seqid = '6'
#             field_list = ['start', 'stop', 'id']
#             seqid_param = 'chromosome'
#             seqid = '6'
#             start_param = 'start'
#             end_param = 'stop'
#             criteria_idx = default_section['CRITERIA_IDX_GENE']
#             criteria_idx_type = section
#         elif feature == 'marker':
#             source_idx_type = 'MARKER'
#             source_idx = ElasticSettings.idx('MARKER', idx_type=source_idx_type)
#             seqid_param = 'seqid'
#             seqid = '6'
#             field_list = ['start', 'end', 'id']
#             start_param = 'start'
#             end_param = 'end'
#             criteria_idx = default_section['CRITERIA_IDX_MARKER']
#             criteria_idx_type = section
# 
#         print(criteria_idx)
#         print(criteria_idx_type)
# 
#         global total_hits
#         total_hits = 0
# 
#         global gl_result_container
#         gl_result_container = {}
# 
#         def process_hits(resp_json):
#             hits = resp_json['hits']['hits']
#             print('===================')
#             print(len(hits))
#             global total_hits
#             total_hits = total_hits + len(hits)
#             print('===================' + str(total_hits))
#             map_exists = False
# 
#             global gl_result_container
#             for hit in hits:
#                 print(hit)
#                 result_container = cls.tag_feature_to_all_diseases(hit['_id'], section, config,
#                                                                    result_container=gl_result_container)
#                 gl_result_container = result_container
#                 print(len(gl_result_container))
# 
#                 if not map_exists:
#                     cls.create_criteria_mapping(criteria_idx, criteria_idx_type)
#                     map_exists = True
#                 cls.load_result_container(result_container, criteria_idx, criteria_idx_type)
#                 print(criteria_idx + ' ' + criteria_idx_type)
#         # Defined MHC region as chr6:25,000,000..35,000,000
#         start_range = 25000000
#         end_range = 35000000
#         ''' Constructs a range overlap query '''
#         query = ElasticUtils.range_overlap_query(seqid, start_range, end_range, field_list, seqid_param,
#                                                  start_param,
#                                                  end_param)
# #         query_bool = BoolQuery(must_arr=[RangeQuery(start_param, lte=start_range),
# #                                          RangeQuery(end_param, gte=end_range)])
# #         or_filter = OrFilter(RangeQuery(start_param, gte=start_range, lte=end_range))
# #         or_filter.extend(RangeQuery(end_param, gte=start_range, lte=end_range)) \
# #                  .extend(query_bool)
# #         query = ElasticQuery.filtered(Query.term(seqid_param, seqid), or_filter, field_list)
#         print(query.__dict__)
#         # return elastic.search().docs
#         ScanAndScroll.scan_and_scroll(source_idx, call_fun=process_hits, query=query)
#         print(total_hits)
#         print(gl_result_container)

    @classmethod
    def get_elastic_query(cls, section=None, config=None):
        section_config = config[section]
        source_fields_str = section_config['source_fields']
        source_fields = source_fields_str.split(',')

        if source_fields is None:
            source_fields = []
        if section == 'is_gene_in_mhc' or section == 'is_marker_in_mhc':
            # Defined MHC region as chr6:25,000,000..35,000,000
            seqid = '6'
            start_range = 25000000
            end_range = 35000000
            field_list = section_config['source_fields']
            seqid_param = section_config['seqid_param']
            start_param = section_config['start_param']
            end_param = section_config['end_param']

            query = ElasticUtils.range_overlap_query(seqid, start_range, end_range,
                                                     field_list,
                                                     seqid_param,
                                                     start_param,
                                                     end_param)
        else:
            query = ElasticQuery(Query.match_all(), sources=source_fields)

        return query

    @classmethod
    def tag_feature_to_all_diseases(cls, feature_id, section, config, result_container={}):

        all_diseases = CriteriaManager.get_available_diseases()
        print(feature_id)
        print(all_diseases)

        result_container_ = result_container
        if config is None:
            print('config is none')
            config = IniParser.read_ini(ini_file='criteria.ini')

        dis_dict = dict()
        criteria_disease_dict = {}

        for disease in all_diseases:
                dis_dict[disease] = []
                criteria_dict = cls.get_criteria_dict(disease, disease)
                if len(result_container_.get(feature_id, {})) > 0:

                    criteria_disease_dict = result_container_[feature_id]
                    criteria_disease_dict = cls.get_criteria_disease_dict([disease], criteria_dict,
                                                                          criteria_disease_dict)

                    result_container_[feature_id] = criteria_disease_dict
                else:
                    criteria_disease_dict = {}
                    criteria_disease_dict = cls.get_criteria_disease_dict([disease], criteria_dict,
                                                                          criteria_disease_dict)
                    result_container_[feature_id] = criteria_disease_dict

        return result_container_

    @classmethod
    def map_and_load(cls, feature, section, config, result_container={}):

        feature_upper = feature.upper()
        criteria_type = 'CRITERIA_IDX_' + feature_upper

        default_section = config['DEFAULT']
        criteria_idx = default_section[criteria_type]
        criteria_idx_type = section

        cls.create_criteria_mapping(criteria_idx, criteria_idx_type)
        cls.load_result_container(result_container, criteria_idx, criteria_idx_type)
        print(criteria_idx + ' ' + criteria_idx_type + ' loaded successfully. DONE')

    @classmethod
    def get_criteria_dict(cls, fid, fname, fnotes={}):

        if(len(fnotes) > 0):
            criteria_dict = {'fid': fid, 'fname': fname, 'fnotes': fnotes}
        else:
            criteria_dict = {'fid': fid, 'fname': fname}

        return criteria_dict

    @classmethod
    def get_criteria_disease_dict(cls, diseases, criteria_dict, criteria_disease_dict):

        for disease in diseases:
            if disease in criteria_disease_dict:
                existing_dict = criteria_disease_dict[disease]
                if criteria_dict not in existing_dict:
                        existing_dict.append(criteria_dict)
                        criteria_disease_dict[disease] = existing_dict
            else:
                criteria_disease_dict[disease] = [criteria_dict]

        return criteria_disease_dict

    @classmethod
    def create_criteria_mapping(cls, idx, idx_type, test_mode=False):

        print('Idx ' + idx)
        print('Idx_type ' + idx_type)
        ''' Create the mapping for alias indexing '''
        props = MappingProperties(idx_type)
        props.add_property("score", "integer")
        props.add_property("disease_tags", "string")
        available_diseases = CriteriaManager().get_available_diseases()

        for disease in available_diseases:
            criteria_tags = MappingProperties(disease)
            criteria_tags.add_property("fid", "string", index="not_analyzed")
            criteria_tags.add_property("fname", "string", index="not_analyzed")
            criteria_tags.add_property("fnotes", "string", index="not_analyzed")
            props.add_properties(criteria_tags)

        ''' create index and add mapping '''
        load = Loader()
        options = {"indexName": idx, "shards": 5}
        if not test_mode:
            load.mapping(props, idx_type, analyzer=Loader.KEYWORD_ANALYZER, **options)
        return props

    @classmethod
    def fetch_overlapping_features(cls, build, seqid, start, end, idx=None, disease_id=None):
        nbuild = build
        start_range = start
        end_range = end

        bool_range = BoolQuery()
        bool_range.must(RangeQuery("build_info.start", lte=start_range)) \
                  .must(RangeQuery("build_info.end", gte=end_range))

        or_filter = OrFilter(RangeQuery("build_info.start", gte=start_range, lte=end_range))

        or_filter.extend(RangeQuery("build_info.end", gte=start_range, lte=end_range)) \
                 .extend(bool_range)

        bool_query = BoolQuery()

        if disease_id:
            qnested_buildinfo = Query.nested('build_info', bool_query)
            bool_query = BoolQuery()
            bool_query.must(Query.term("disease", disease_id.lower())).must(qnested_buildinfo)
            qnested = ElasticQuery(bool_query, sources=['build_info.*',
                                                        'disease_locus',
                                                        'disease',
                                                        'chr_band',
                                                        'species'])

        else:
            bool_query.must(Query.term("build_info.build", nbuild)) \
                  .must(Query.term("build_info.seqid", seqid)) \
                  .filter(or_filter)

            qnested = ElasticQuery(Query.nested('build_info', bool_query), sources=['build_info.*',
                                                                                    'disease_locus',
                                                                                    'disease',
                                                                                    'chr_band',
                                                                                    'species'])

        elastic = Search(qnested, idx=idx)
        res = elastic.search()
        return res

    @classmethod
    def calculate_score(cls, disease_list):

        core_diseases = CriteriaManager.get_available_diseases(1)
        other_diseases = CriteriaManager.get_available_diseases(2)

        score = 0
        for disease_key in disease_list:

            if disease_key in core_diseases:
                score += 10
            elif disease_key in other_diseases:
                score += 5
            else:
                score += 0
        return score

    @classmethod
    def load_result_container(cls, result_container, idx, idx_type):

        json_data = ''
        line_num = 0
        for feature_id in result_container:

            if feature_id is None:
                continue

            row_obj = {"index": {"_index": idx, "_type": idx_type, "_id": feature_id}}
            row = result_container[feature_id]
            disease_tags = list(row.keys())

            if 'score' in disease_tags:
                disease_tags.remove('score')
            if 'disease_tags' in disease_tags:
                disease_tags.remove('disease_tags')

            score = cls.calculate_score(disease_tags)
            row['score'] = score
            row['disease_tags'] = disease_tags

            json_data += json.dumps(row_obj) + '\n'
            json_data += json.dumps(row) + '\n'

            line_num += 1

            if(line_num > 5000):
                line_num = 0
                print('.', end="", flush=True)
                Loader().bulk_load(idx, idx_type, json_data)
                json_data = ''
        if line_num > 0:
            Loader().bulk_load(idx, idx_type, json_data)

