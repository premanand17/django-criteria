import json
import logging

from criteria.helper.criteria_manager import CriteriaManager
from data_pipeline.utils import IniParser
from elastic.aggs import Agg, Aggs
from elastic.elastic_settings import ElasticSettings
from elastic.management.loaders.loader import Loader
from elastic.management.loaders.mapping import MappingProperties
from elastic.query import BoolQuery, RangeQuery, OrFilter, Query
from elastic.search import Search, ElasticQuery, ScanAndScroll, Highlight
from elastic.utils import ElasticUtils
from disease.utils import Disease
from django.conf import settings
from region.utils import Region
import re


logger = logging.getLogger(__name__)


class Criteria():
    ''' Criteria class implementing common functions for all criteria types  '''

    (main_codes, other_codes) = CriteriaManager.get_available_diseases()
    site_enabled_diseases = main_codes + other_codes
    test_mode = False
    gl_result_container = None

    global hit_counter
    hit_counter = 0

    @classmethod
    def process_criteria(cls, feature, section, config, sub_class, test=False):
        ''' Top level function that calls the right criteria implementation based on the subclass passed. Iterates over all the
            documents using the ScanAndScroll and the hits are processed by the inner function process_hits.
            The entire result is stored in result_container (a dict), and at the end of the processing, the result is
            loaded in to the elastic index after creating the mapping
        @type  feature: string
        @param feature: feature type, could be 'gene','region', 'marker' etc.,
        @type  section: string
        @keyword section: The section in the criteria.ini file
        @type  config:  string
        @keyword config: The config object initialized from criteria.ini.
        @type  sub_class: string
        @param sub_class: The name of the inherited sub_class where the actual implementation is
        '''
        global gl_result_container
        gl_result_container = {}
        test_mode = test
        if config is None:
            if test_mode:
                config = CriteriaManager().get_criteria_config(ini_file='test_criteria.ini')
            else:
                config = CriteriaManager().get_criteria_config(ini_file='criteria.ini')

        section_config = config[section]
        source_idx = section_config['source_idx']

        if ',' in source_idx:
            idxs = source_idx.split(',')
            idx_all = [ElasticSettings.idx(idx) for idx in idxs]
            source_idx = ','.join(idx_all)
        else:
            source_idx = ElasticSettings.idx(section_config['source_idx'])

        source_idx_type = None
        if 'source_idx_type' in section_config:
            source_idx_type = section_config['source_idx_type']

        if source_idx_type is not None:
            source_idx = ElasticSettings.idx(section_config['source_idx'], idx_type=section_config['source_idx_type'])
        else:
            source_idx_type = ''

        logger.warning(source_idx + ' ' + source_idx_type)

        def process_hits(resp_json):
            global gl_result_container
            hits = resp_json['hits']['hits']
            global hit_counter
            for hit in hits:
                # print('======HIT COUNTER====' + str(hit_counter))
                hit_counter = hit_counter + 1

                result_container = sub_class.tag_feature_to_disease(hit, section, config,
                                                                    result_container=gl_result_container)
                gl_result_container = result_container

                if test_mode:
                    if gl_result_container is not None and len(gl_result_container) > 5:
                        return

        query = cls.get_elastic_query(section, config)

        if test_mode:
            result_size = len(gl_result_container)
            from_ = 0
            size_ = 20
            while (result_size < 1):
                from_ = from_ + size_
                url = ElasticSettings.url()
                if 'mhc' in section:
                    url_search = (source_idx + '/_search')
                else:
                    url_search = (source_idx + '/_search?from=' + str(from_) + '&size=' + str(size_))

                if query is None:
                    query = {
                              "query": {"match_all": {}},
                              "size":  20
                              }
                    response = Search.elastic_request(url, url_search, data=json.dumps(query))
                    query = None
                else:
                    # print(query)
                    response = Search.elastic_request(url, url_search, data=json.dumps(query.query))

                process_hits(response.json())
                if gl_result_container is not None:
                    result_size = len(gl_result_container)
        else:
            ScanAndScroll.scan_and_scroll(source_idx, call_fun=process_hits, query=query)

        cls.map_and_load(feature, section, config, gl_result_container)

    @classmethod
    def get_elastic_query(cls, section=None, config=None):
        ''' function to build the elastic query object
        @type  section: string
        @keyword section: The section in the criteria.ini file
        @type  config:  string
        @keyword config: The config object initialized from criteria.ini.
        @return: L{Query}
        '''
        section_config = config[section]
        source_fields = []

        if 'source_fields' in section_config:
            source_fields_str = section_config['source_fields']
            source_fields = source_fields_str.split(',')

        if 'mhc' in section:
            seqid = '6'
            start_range = 25000000
            end_range = 35000000

            seqid_param = section_config['seqid_param']
            start_param = section_config['start_param']
            end_param = section_config['end_param']

        if section == 'is_gene_in_mhc':
            # for region you should make a different query
            # Defined MHC region as chr6:25,000,000..35,000,000

            query = ElasticUtils.range_overlap_query(seqid, start_range, end_range,
                                                     source_fields,
                                                     seqid_param,
                                                     start_param,
                                                     end_param)
        elif section == 'is_marker_in_mhc':
            query_bool = BoolQuery()
            query_bool.must(RangeQuery("start", lte=end_range)) \
                      .must(RangeQuery("start", gte=start_range)) \
                      .must(Query.term("seqid", seqid))
            query = ElasticQuery.filtered_bool(Query.match_all(), query_bool, sources=["id", "seqid", "start"])
        elif section == 'is_region_in_mhc':
            query = ElasticQuery(Query.term("region_name", "MHC"))
        elif section == 'marker_is_gwas_significant_in_ic':
            # build a range query
            gw_sig_p = 0.00000005
            query = ElasticQuery(RangeQuery("p_value", lte=gw_sig_p))
        else:
            if len(source_fields) > 0:
                query = ElasticQuery(Query.match_all(), sources=source_fields)
            else:
                # query = ElasticQuery(Query.match_all())
                return None

        return query

    @classmethod
    def tag_feature_to_all_diseases(cls, feature_id, section, config, result_container={}):
        ''' function to tag the feature to all the diseases, used to tag features in the MHC region
        @type  feature_id: string
        @keyword feature_id: Id of the feature (gene => gene_id, region=>region_id)
        @type  section: string
        @keyword section: The section in the criteria.ini file
        @type  config:  string
        @keyword config: The config object initialized from criteria.ini.
        @type result_container : string
        @keyword result_container: Container object for storing the result with keys as the feature_id
        '''
#         (main_codes, other_codes) = CriteriaManager.get_available_diseases()
#         all_diseases = main_codes + other_codes

        result_container_ = result_container
        if config is None:
            config = IniParser.read_ini(ini_file='criteria.ini')

        dis_dict = dict()
        criteria_disease_dict = {}

        for disease in cls.site_enabled_diseases:
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
        ''' function to map and load the results in to elastic index
        @type  feature: string
        @param feature: feature type, could be 'gene','region', 'marker' etc.,
        @type  section: string
        @keyword section: The section in the criteria.ini file
        @type  config:  string
        @keyword config: The config object initialized from criteria.ini.
        @type result_container : string
        @keyword result_container: Container object for storing the result with keys as the feature_id
        '''
        feature_upper = feature.upper()
        criteria_type = 'CRITERIA_IDX_' + feature_upper

        default_section = config['DEFAULT']
        criteria_idx = default_section[criteria_type]
        criteria_idx_type = section

        cls.create_criteria_mapping(criteria_idx, criteria_idx_type)
        cls.load_result_container(result_container, criteria_idx, criteria_idx_type)
        logger.warning(criteria_idx + ' ' + criteria_idx_type + ' loaded successfully. DONE')

    @classmethod
    def get_criteria_dict(cls, fid, fname, fnotes={}):
        ''' function to create a criteria_dict initialized with fid, fname, and fnotes
        @type  fid: string
        @param fid: feature id
        @type  fname: string
        @param fname: feature name
        @type  fnotes:  string
        @keyword fnotes: fnotes eg: {'linkdata': 'rsq', 'linkvalue': rsquared,
                                     'linkid': dil_study_id, 'linkname': first_author}
        '''
        if(fnotes is not None and len(fnotes) > 0):
            criteria_dict = {'fid': fid, 'fname': fname, 'fnotes': fnotes}
        else:
            criteria_dict = {'fid': fid, 'fname': fname}

        return criteria_dict

    @classmethod
    def get_criteria_disease_dict(cls, diseases, criteria_dict, criteria_disease_dict):
        ''' function to create a dict object with key as diseases and values as a dict with fid, fname, and fnotes
        @type  diseases: string
        @param diseases: list of diseases to tag with
        @type  criteria_dict: string
        @keyword criteria_dict: dict with keys as disease code and values as fname, fid, fnotes
                                eg: {'T1D': [{'fname': 'Barrett', 'fid': 'GDXHsS00004'}]}
        @type  criteria_disease_dict:  string
        @keyword criteria_disease_dict: fnotes eg: {'linkdata': 'rsq', 'linkvalue': rsquared,
                                     'linkid': dil_study_id, 'linkname': first_author}
        '''
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
        ''' function to create mapping for criteria indexes
        @type  idx: string
        @param idx: name of the index
        @type  idx_type: string
        @param idx_type: name of the idx type, each criteria is an index type
        @type  test_mode:  string
        @param test_mode: flag to create or not create the mapping
        '''
        logger.warning('Idx ' + idx)
        logger.warning('Idx_type ' + idx_type)
        ''' Create the mapping for alias indexing '''
        props = MappingProperties(idx_type)
        props.add_property("score", "integer")
        props.add_property("disease_tags", "string", index="not_analyzed")
        props.add_property("qid", "string", index="not_analyzed")
        (main_codes, other_codes) = CriteriaManager().get_available_diseases()

        for disease in main_codes + other_codes:
            criteria_tags = MappingProperties(disease)
            criteria_tags.add_property("fid", "string", index="not_analyzed")
            criteria_tags.add_property("fname", "string", index="not_analyzed")

            fnotes = MappingProperties('fnotes')
            fnotes.add_property('linkid', "string", index="not_analyzed")
            fnotes.add_property('linkname', "string", index="not_analyzed")
            fnotes.add_property('linkdata', "string", index="not_analyzed")
            fnotes.add_property('linkvalue', "string", index="not_analyzed")
            criteria_tags.add_properties(fnotes)
            props.add_properties(criteria_tags)

        ''' create index and add mapping '''
        load = Loader()
        options = {"indexName": idx, "shards": 5}

        '''add meta info'''
        config = CriteriaManager.get_criteria_config()
        idx_type_cfg = config[idx_type]
        desc = idx_type_cfg['desc']
        meta = {"desc": desc}
        if not test_mode:
            load.mapping(props, idx_type, meta=meta, analyzer=Loader.KEYWORD_ANALYZER, **options)
        return props

    @classmethod
    def fetch_overlapping_features(cls, build, seqid, start, end, idx=None, idx_type=None, disease_id=None):
        ''' function to create fetch overlapping features for a given stretch of region
            the build info is stored as nested document..so nested query is build
        @type  build: string
        @param build: build info eg: 'GRCh38'
        @type  seqid: string
        @param seqid: chromosome number
        @type  start:  string
        @param start: region start
        @type  end:  string
        @param end: region end
        @type  idx: string
        @param idx: name of the index
        @type  idx_type: string
        @param idx_type: name of the idx type, each criteria is an index type
        @type  disease_id:  string
        @param disease_id: disease code
        '''
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

        elastic = Search(qnested, idx=idx, idx_type=idx_type)
        res = elastic.search()
        return res.docs

    @classmethod
    def calculate_score(cls, disease_list):
        ''' function to calculate score based on the disease tiers...core diseases gets 10 and non-score gets 5
        @type  disease_list: string
        @param disease_list: list of disease codes eg: ['T1D', 'MS', 'AA']
        '''
        core_diseases = cls.main_codes
        other_diseases = cls.other_codes

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
        ''' function to load the results in to index using the bulk loader
        @type result_container : string
        @keyword result_container: Container object for storing the result with keys as the feature_id
        @type  idx: string
        @param idx: name of the index
        @type  idx_type: string
        @param idx_type: name of the idx type, each criteria is an index type
        '''
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
            row['qid'] = feature_id

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

    @classmethod
    def populate_container(cls, fid, fname, fnotes=None, features=None, diseases=None, result_container={}):
        ''' function to populate the result container with the results
        @type  fid: string
        @param fid: feature id
        @type  fname: string
        @param fname: feature name
        @type  fnotes:  string
        @keyword fnotes: fnotes eg: {'linkdata': 'rsq', 'linkvalue': rsquared,
                                     'linkid': dil_study_id, 'linkname': first_author}
                                            @type  fid: string
        @type  features: string
        @param features: list of feature_ids
        @type  diseases: string
        @param diseases: list of diseases
        @type result_container : string
        @keyword result_container: Container object for storing the result with keys as the feature_id
        '''

        result_container_ = result_container

        criteria_dict = cls.get_criteria_dict(fid, fname, fnotes)

        dis_dict = dict()
        criteria_disease_dict = {}
        for feature in features:

            if feature is None:
                continue

            for disease in diseases:

                dis_dict[disease] = []
                if len(result_container_.get(feature, {})) > 0:

                    criteria_disease_dict = result_container_[feature]
                    criteria_disease_dict = cls.get_criteria_disease_dict(diseases, criteria_dict,
                                                                          criteria_disease_dict)

                    result_container_[feature] = criteria_disease_dict
                else:
                    criteria_disease_dict = {}
                    criteria_disease_dict = cls.get_criteria_disease_dict(diseases, criteria_dict,
                                                                          criteria_disease_dict)
                    result_container_[feature] = criteria_disease_dict

        return result_container_

    @classmethod
    def get_available_criterias(cls, feature=None, config=None, test=False):
        ''' function to get avalable criterias for a given feature
        @type  feature: string
        @param feature: feature type, could be 'gene','region', 'marker' etc.,
        @type  config:  string
        @keyword config: The config object initialized from criteria.ini.
        '''
        if config is None:
            if test:
                config = CriteriaManager.get_criteria_config(ini_file='test_criteria.ini')
            else:
                config = CriteriaManager.get_criteria_config(ini_file='criteria.ini')

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
    def get_disease_tags(cls, feature_id, idx=None, idx_type=None):
        ''' function to get the aggregated list of disease_tags for a given feature id, aggregated
            from all criteria_types for a feature type
        @type  feature_id: string
        @keyword feature_id: Id of the feature (gene => gene_id, region=>region_id)
              @type  idx: string
        @param idx: name of the index
        @type  idx_type: string
        @param idx_type: name of the idx type, each criteria is an index type
        '''
        query = ElasticQuery(Query.term("qid", feature_id))
        agg = Agg("criteria_disease_tags", "terms", {"field": "disease_tags", "size": 0})
        aggs = Aggs(agg)

        if idx_type:
            search = Search(query, aggs=aggs, idx=idx, idx_type=idx_type)
        else:
            search = Search(query, aggs=aggs, idx=idx)

        disease_tags = []
        try:
            r_aggs = search.search().aggs
            buckets = r_aggs['criteria_disease_tags'].get_buckets()
            disease_tags = [dis_dict['key'].lower() for dis_dict in buckets]
        except:
            return []

        # get disease docs
        if (len(disease_tags) > 0):
            (core, other) = Disease.get_site_diseases(dis_list=disease_tags)
            diseases = list(core)
            diseases.extend(other)
            return diseases
        else:
            return None

    @classmethod
    def get_criteria_details(cls, feature_id, idx, idx_type, criteria_id=None):
        '''Function to get criteria details for a given feature_id. If criteria_id is given,
        the result is restricted to that criteria
        @type  feature_id: string
        @keyword feature_id: Id of the feature (gene => gene_id, region=>region_id)
        @type  criteria_id: string
        @keyword criteria_id: criteria_id eg: cand_gene_in_study, gene_in_region
        @type  idx: string
        @param idx: name of the index
        @type  idx_type: string
        @param idx_type: name of the idx type, each criteria is an index type
        '''
        query = ElasticQuery(Query.term("qid", feature_id))
        search = Search(query, idx=idx, idx_type=idx_type)
#        elastic_docs = search.search().docs
        criteria_hits = search.get_json_response()['hits']
        return(criteria_hits)

    @classmethod
    def get_all_criteria_disease_tags(cls, qids, idx, idx_type):

        query = ElasticQuery(Query.terms("qid", qids), sources=['disease_tags', 'qid'])

        search = Search(query, idx=idx, idx_type=idx_type)
        criteria_hits = search.get_json_response()['hits']
        hits = criteria_hits['hits']

        meta_info = {}

        criteria_disease_tags = {}
        for hit in hits:
            if idx == hit['_index']:
                qid = hit['_source']['qid']
                meta_desc = cls.get_meta_desc(idx, [hit['_type']])
                meta_info[hit['_type']] = meta_desc[idx][hit['_type']]

                criteria_desc = hit['_type']

                if qid not in criteria_disease_tags:
                    criteria_disease_tags[qid] = {}
                criteria_disease_tags[qid][criteria_desc] = hit['_source']['disease_tags']

        disease_tags_all = []
        for fid, fvalue in criteria_disease_tags.items():
            disease_tags_all = cls.get_all_criteria_disease_tags_aggregated(qid, fvalue)
            criteria_disease_tags[fid]['all'] = disease_tags_all

            criteria_disease_tags[fid]['meta_info'] = meta_info

        return(criteria_disease_tags)

    @classmethod
    def get_all_criteria_disease_tags_aggregated(cls, qid, criteria_disease_tags):

        disease_tags_all = []
        for ftype in criteria_disease_tags:
            disease_tags = criteria_disease_tags[ftype]
            disease_tags_all.extend(disease_tags)

        uniq_list = set(disease_tags_all)
        return list(uniq_list)

    @classmethod
    def get_meta_info(cls, idx, idx_type):
        elastic_url = ElasticSettings.url()
        meta_url = idx + '/' + idx_type + '/_mapping'
        # print(elastic_url + meta_url)
        meta_response = Search.elastic_request(elastic_url, meta_url, is_post=False)

        try:
            elastic_meta = json.loads(meta_response.content.decode("utf-8"))
            meta_info = elastic_meta[idx]['mappings'][idx_type]['_meta']
            return meta_info
        except:
            return None

    @classmethod
    def get_meta_desc(cls, idx, criteria_list):
        meta_desc = {}
        for criteria in criteria_list:
            meta_info = cls.get_meta_info(idx, criteria)
            if meta_info is not None:
                desc = meta_info['desc']

                if idx not in meta_desc:
                    meta_desc[idx] = {}

                meta_desc[idx][criteria] = desc

        return meta_desc

    @classmethod
    def get_link_info(cls, idx, criteria_list):

        link_info = {}

        criteria_config = CriteriaManager.get_criteria_config()
        for criteria in criteria_list:
            criteria_section = criteria_config[criteria]

            if idx not in link_info:
                    link_info[idx] = {}

            if 'link_to_feature' in criteria_section:
                link_to_feature = criteria_section['link_to_feature']
                link_info[idx][criteria] = link_to_feature

        return link_info

    @classmethod
    def get_disease_codes_from_results(cls, idx, criteria_results):

        hits = criteria_results['hits']
        disease_codes = []
        for hit in hits:
            if 'disease_tags' in hit['_source']:
                dis_tag = hit['_source']['disease_tags']
                disease_codes.extend(dis_tag)

        uniq_list = set(disease_codes)
        return list(uniq_list)

    @classmethod
    def add_meta_info(cls, idx, criteria_list, result_dict):

        meta_desc = Criteria.get_meta_desc(idx, criteria_list)
        if len(meta_desc) > 0:
            result_dict['meta_info'] = meta_desc

        # get the link_info and attach to the result_dict
        link_info = Criteria.get_link_info(idx, criteria_list)
        if len(link_info) > 0:
            result_dict['link_info'] = link_info

        agg_disease_tags = Criteria.get_disease_codes_from_results(idx, result_dict)
        if len(agg_disease_tags) > 0:
            result_dict['agg_disease_tags'] = agg_disease_tags

        return result_dict

    @classmethod
    def get_feature_idx_n_idxtypes(cls, feature_type):

        idx = ElasticSettings.idx(feature_type.upper()+'_CRITERIA')

        available_criterias = cls.get_available_criterias(feature=feature_type)
        criteria_list = available_criterias[feature_type]
        idx_type = ','.join(criteria_list)

        return (idx, idx_type)

    @classmethod
    def do_identifier_search(cls, identifiers, user=None):

        source_filter = [
                        'symbol', 'synonyms', "dbxrefs.*",                            # gene
                        'id', 'rscurrent', 'rshigh',                                  # marker
                        'study_id', 'study_name',                                     # study
                        'region_name', 'marker', "region_id"]                                      # regions

        highlight = Highlight(["symbol", "dbxrefs.*", "region", "region_name","region_id",
                               "study_id", "study_name", "id", "rscurrent", "rshigh", "marker"])

        search_query = ElasticQuery(Query.query_string(" ".join(identifiers), fields=source_filter),
                                    highlight=highlight, sources=source_filter)

        search_idx_keys = ['REGION', 'GENE', 'STUDY', 'MARKER']
        search_idx_type_keys = ['REGION', 'GENE',  'STUDY', 'MARKER']

        idx_all = [ElasticSettings.idx_names(idx, idx_type=idx_type) for idx, idx_type in zip(search_idx_keys,
                                                                                              search_idx_type_keys)]
        idx_dict = dict(idx_all)

        search_idx = ','.join(idx_dict.keys())
        search_idx_types = ','.join(idx_dict.values())

        print(search_idx)
        print(search_idx_types)

        elastic = Search(search_query=search_query, idx=search_idx, idx_type=search_idx_types)

        gene_dict = {}
        region_dict = {}
        marker_dict = {}
        study_dict = {}

        docs = elastic.search().docs
        for doc in docs:
            existing_feature_list = []

            idx = getattr(doc, '_meta')['_index']
            idx_type = getattr(doc, '_meta')['_type']
            doc_id = doc.doc_id()
            print(idx)
            print(idx_type)
            print('============')
            print(doc_id)

            highlight = doc.highlight()
            if highlight is not None:
                pattern = ".*?<em>(.*?)</em>.*"
                result = re.match(pattern, str(highlight))
                if result is not None:
                    highlight_hit = result.group(1)

                    if idx_type == "studies":
                        feature_id = getattr(doc, "study_id")

                        if highlight_hit not in study_dict:
                            study_dict[highlight_hit] = {}

                        if feature_id in study_dict[highlight_hit]:
                            existing_feature_list = study_dict[highlight_hit]

                        existing_feature_list.append(feature_id)
                        study_dict[highlight_hit] = existing_feature_list

                    if idx_type == "gene":
                        feature_id = doc_id

                        if highlight_hit not in gene_dict:
                            gene_dict[highlight_hit] = {}

                        if feature_id in gene_dict[highlight_hit]:
                            existing_feature_list = gene_dict[highlight_hit]

                        existing_feature_list.append(feature_id)
                        gene_dict[highlight_hit] = existing_feature_list

                    if idx_type == "marker":
                        feature_id = getattr(doc, "id")

                        if highlight_hit not in marker_dict:
                            marker_dict[highlight_hit] = {}

                        if feature_id in marker_dict[highlight_hit]:
                            existing_feature_list = marker_dict[highlight_hit]

                        existing_feature_list.append(feature_id)
                        marker_dict[highlight_hit] = existing_feature_list

                    if idx_type == "region":
                        feature_id = getattr(doc, "region_id")

                        if highlight_hit not in region_dict:
                            region_dict[highlight_hit] = {}

                        if feature_id in region_dict[highlight_hit]:
                            existing_feature_list = region_dict[highlight_hit]

                        existing_feature_list.append(feature_id)
                        region_dict[highlight_hit] = existing_feature_list

        print(study_dict)
        print(gene_dict)
        print(marker_dict)
        print(region_dict)

        all_result_dict = {}
        all_result_dict['gene'] = gene_dict
        all_result_dict['marker'] = marker_dict
        all_result_dict['region'] = region_dict
        all_result_dict['study'] = study_dict


        print('===================')
        original_list = [_id.lower() for _id in identifiers]
        print(original_list)
        result_list = list(study_dict.keys()) + list(gene_dict.keys()) + list(marker_dict.keys()) + list(region_dict.keys())
        result_list = [_id.lower() for _id in result_list]
        print(result_list)
        diff_list = set(original_list) - set(result_list)
        print(diff_list)
        all_result_dict['missing'] = list(diff_list)
        print('+++++++++++++++++++')

        print(all_result_dict)
        return all_result_dict

    @classmethod
    def do_criteria_search(cls, identifiers, user=None):

        all_result_dict = cls.do_identifier_search(identifiers, user)
        criteria_disease_tags = {}
        for feature_type in all_result_dict:
            if feature_type != 'missing':
                print(feature_type)
                feature_dict = {}
                for queryid, querynames in all_result_dict[feature_type].items():
                    if feature_type in feature_dict:
                        existing_list = feature_dict[feature_type]
                        existing_list.extend(querynames)
                    else:
                        feature_dict[feature_type] = querynames

               
                (idx, idx_types) = cls.get_feature_idx_n_idxtypes(feature_type)
                if feature_type in feature_dict:
                    criteria_disease_tags[feature_type] = cls.get_all_criteria_disease_tags(feature_dict[feature_type], idx, idx_types)

        return criteria_disease_tags


    @classmethod
    def _collapse_region_docs(cls, docs):
        ''' If the document is a hit then find parent region; pad all regions for build_info.'''
        hits = [doc for doc in docs if doc.type() == 'hits']
        regions = [doc for doc in docs if doc.type() == 'region']

        if len(hits) > 0:
            regions = Region.hits_to_regions(hits)
            return regions
