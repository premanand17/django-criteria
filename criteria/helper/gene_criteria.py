import logging
import json
from builtins import classmethod
from elastic.search import ScanAndScroll, ElasticQuery, Search
from elastic.query import Query, BoolQuery, RangeQuery, OrFilter
from elastic.management.loaders.mapping import MappingProperties
from elastic.management.loaders.loader import Loader
from criteria.helper.criteria_manager import CriteriaManager
from elastic.elastic_settings import ElasticSettings
from data_pipeline.utils import IniParser

logger = logging.getLogger(__name__)


class GeneCriteria():

    ''' GeneCriteria class define functions for building gene index type within criteria index

    '''

    @classmethod
    def process_gene_criteria(cls, section, config):

        if config is None:
            config = CriteriaManager().get_criteria_config()

        section_config = config[section]
        source_idx = ElasticSettings.idx(section_config['source_idx'])
        source_idx_type = section_config['source_idx_type']
        source_fields_str = section_config['source_fields']
        source_fields = source_fields_str.split(',')

        if source_idx_type is not None:
            source_idx = ElasticSettings.idx(section_config['source_idx'], idx_type=section_config['source_idx_type'])
        else:
            source_idx_type = ''

        if source_fields is None:
            source_fields = []

        logger.warn(source_idx + ' ' + source_idx_type + ' ' + str(source_fields))

        global gl_result_container
        gl_result_container = {}

        def process_hits(resp_json):
            map_exists = False
            hits = resp_json['hits']['hits']
            global gl_result_container
            for hit in hits:
                result_container = cls.tag_feature_to_disease(hit['_source'], section, config,
                                                              result_container=gl_result_container)
                gl_result_container = result_container
                print(len(gl_result_container))
                default_section = config['DEFAULT']
                criteria_idx = default_section['CRITERIA_IDX_GENE']
                criteria_idx_type = section
                if not map_exists:
                    cls.create_criteria_mapping(criteria_idx, criteria_idx_type)
                    map_exists = True
                cls.load_result_container(result_container, criteria_idx, criteria_idx_type)
                print(criteria_idx + ' ' + criteria_idx_type)

        # qbool = BoolQuery().must([Query.terms("study_id", ["GDXHsS00012", "GDXHsS00027", "GDXHsS00004"])])
        qbool = None
        if qbool:
            query = ElasticQuery.filtered_bool(Query.match_all(), qbool, sources=source_fields)
        else:
            query = ElasticQuery(Query.match_all(), sources=source_fields)

        ScanAndScroll.scan_and_scroll(source_idx, call_fun=process_hits, query=query)

    @classmethod
    def load_result_container(cls, result_container, idx, idx_type):

        json_data = ''
        line_num = 0
        for gene in result_container:

            if gene is None:
                continue

            row_obj = {"index": {"_index": idx, "_type": idx_type, "_id": gene}}
            row = result_container[gene]
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

    @classmethod
    def tag_feature_to_disease(cls, feature_doc, section, config, result_container={}):
        feature_class = 'GeneCriteria'
        # Get class from globals and create an instance
        m = globals()[feature_class]()
        # Get the function (from the instance) that we need to call
        func = getattr(m, section)
        result_container_ = func(feature_doc, config, result_container=result_container)
        return result_container_

    @classmethod
    def cand_gene_in_study(cls, feature_doc, config=None, result_container={}):

        result_container_ = result_container
        if config is None:
            print('config is none')
            config = IniParser.read_ini(ini_file='criteria.ini')

        genes = feature_doc['genes']
        diseases = feature_doc['diseases']
        study_id = feature_doc['study_id']
        author = feature_doc['authors'][0]
        first_author = author['name'] + ' ' + author['initials']
        print('Number of genes for study id ' + study_id + '  genes ' +
              str(len(genes)) + str(diseases) + first_author)
        criteria_dict = cls.get_criteria_dict(study_id, first_author)

        dis_dict = dict()
        criteria_disease_dict = {}
        for gene in genes:
            if gene is None:
                continue

            for disease in diseases:
                dis_dict[disease] = []
                if len(result_container_.get(gene, {})) > 0:

                    criteria_disease_dict = result_container_[gene]
                    criteria_disease_dict = cls.get_criteria_disease_dict(diseases, criteria_dict,
                                                                          criteria_disease_dict)

                    result_container_[gene] = criteria_disease_dict
                else:
                    criteria_disease_dict = {}
                    criteria_disease_dict = cls.get_criteria_disease_dict(diseases, criteria_dict,
                                                                          criteria_disease_dict)
                    result_container_[gene] = criteria_disease_dict

        return result_container_

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
    def gene_in_region(cls, feature_src, config=None, details=True, disease_id=None):
        '''Function to process the criteria cand_gene_in_region'''
        gene_index = ElasticSettings.idx('GENE', idx_type='GENE')
        region_index = ElasticSettings.idx('REGIONS')

        if type(feature_src) == dict:
            feature_id = str(feature_src['_id'])
        else:
            feature_id = feature_src
            feature_src = dict()
            query = ElasticQuery(Query.ids([feature_id]))
            elastic = Search(query, idx=gene_index)
            docs = elastic.search().docs

            if(len(docs) == 1):
                doc = docs[0]
                feature_src['chromosome'] = getattr(doc, 'chromosome')
                feature_src['start'] = getattr(doc, 'start')
                feature_src['stop'] = getattr(doc, 'stop')

        result = cls.fetch_overlapping_features('38', feature_src['chromosome'],
                                                feature_src['start'],
                                                feature_src['stop'],
                                                idx=region_index, disease_id=disease_id)

        disease_loc_docs = cls.fetch_disease_locus(result)

        if details is False:
            disease_list = {getattr(doc, 'disease') for doc in disease_loc_docs}
            return disease_list
        else:
            locus_dict = dict()
            disease_tags = set()
            for doc in disease_loc_docs:
                disease = getattr(doc, 'disease')
                disease_tags.add(disease)
                region_name = getattr(doc, 'region_name')
                region_id = doc.doc_id()

                tmp_dict = {'fid': region_id, 'fname': region_name}
                if disease in locus_dict:
                    existing_list = locus_dict[disease]
                    existing_list.append(tmp_dict)
                    locus_dict[disease] = existing_list
                else:
                    new_list = []
                    new_list.append(tmp_dict)
                    locus_dict[disease] = new_list

            if(len(locus_dict) > 0):
                score = cls.calculate_score(list(disease_tags))
                locus_dict['score'] = score
                locus_dict['disease_tags'] = list(disease_tags)
                return locus_dict
            else:
                return None

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

        core_diseases = CriteriaManager().get_available_diseases(1)
        other_diseases = CriteriaManager().get_available_diseases(2)

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
    def fetch_disease_locus(cls, hits_docs):

        region_index = ElasticSettings.idx('REGIONS', idx_type='DISEASE_LOCUS')
        disease_loc_docs = []
        locus_id_set = set()
        for doc in hits_docs.docs:
                locus_id = getattr(doc, 'disease_locus')
                if locus_id not in locus_id_set:
                    locus_id_set.add(locus_id)
                    query = ElasticQuery(Query.ids([locus_id]))
                    elastic = Search(query, idx=region_index)
                    disease_loc = elastic.search().docs
                    if(len(disease_loc) == 1):
                        disease_loc_docs.append(disease_loc[0])
                    else:
                        logger.critical('disease_locus doc not found for it ' + locus_id)

        return disease_loc_docs

    @classmethod
    def get_available_criterias(cls, feature_id, criteria_index):

        query = ElasticQuery(Query.ids([feature_id]))
        elastic = Search(query, idx=criteria_index)
        docs = elastic.search().docs

        formatted_disease_tags = {}
        if(len(docs) == 1):
            # process
            result_doc = docs[0]
            disease_tags = getattr(result_doc, 'disease_tags')
            formatted_disease_tags = cls.format_disease_tags(disease_tags)
            return formatted_disease_tags
        elif(len(docs) > 1):
            logger.critical('More than one doc found with same id ' + feature_id)
        else:
            logger.warning('No doc found for the feature_id ' + feature_id)
            # No docs found

    @classmethod
    def format_disease_tags(cls, disease_tags):

        formatted_disease_tags = {criteria_dict['disease']: criteria_dict['criteria'] for criteria_dict in disease_tags}
        return formatted_disease_tags
