import logging
import json
from builtins import classmethod
from elastic.search import ScanAndScroll, ElasticQuery, Search
from elastic.query import Query
from elastic.management.loaders.mapping import MappingProperties
from elastic.management.loaders.loader import Loader
from criteria.helper.criteria_manager import CriteriaManager
from elastic.elastic_settings import ElasticSettings
from data_pipeline.utils import IniParser
from criteria.helper.criteria import Criteria
from test.test_pyclbr import ClassMethodType
from elastic.utils import ElasticUtils

logger = logging.getLogger(__name__)


class GeneCriteria(Criteria):

    ''' GeneCriteria class define functions for building gene index type within criteria index

    '''

    @classmethod
    def process_gene_criteria(cls, feature, section, config):

        if config is None:
            config = CriteriaManager().get_criteria_config()

        section_config = config[section]
        source_idx = ElasticSettings.idx(section_config['source_idx'])
        source_idx_type = section_config['source_idx_type']

        if source_idx_type is not None:
            source_idx = ElasticSettings.idx(section_config['source_idx'], idx_type=section_config['source_idx_type'])
        else:
            source_idx_type = ''

        logger.warn(source_idx + ' ' + source_idx_type)

        global gl_result_container
        gl_result_container = {}

        def process_hits(resp_json):
            hits = resp_json['hits']['hits']
            global gl_result_container
            for hit in hits:
                source = hit['_source']
                source['_id'] = hit['_id']
                result_container = cls.tag_feature_to_disease(source, section, config,
                                                              result_container=gl_result_container)
                gl_result_container = result_container
                print(len(gl_result_container))

        query = cls.get_elastic_query(section, config)

        ScanAndScroll.scan_and_scroll(source_idx, call_fun=process_hits, query=query)
        cls.map_and_load(feature, section, config, gl_result_container)

    @classmethod
    def cand_gene_in_study(cls, feature_doc, section=None, config=None, result_container={}):

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
    def tag_feature_to_disease(cls, feature_doc, section, config, result_container={}):
        feature_class = cls.__name__
        # Get class from globals and create an instance
        m = globals()[feature_class]()
        # Get the function (from the instance) that we need to call
        func = getattr(m, section)
        result_container_ = func(feature_doc, section, config, result_container=result_container)
        return result_container_

    @classmethod
    def is_gene_in_mhc(cls, feature_doc, section=None, config=None, result_container={}):
        feature_id = feature_doc['_id']
        print(feature_id)
        result_container_ = cls.tag_feature_to_all_diseases(feature_id, section, config, result_container)
        return result_container_

#     @classmethod
#     def gene_in_region(cls, feature_doc, config=None, result_container={}):
#
#         result_container_ = result_container
#         if config is None:
#             print('config is none')
#             config = IniParser.read_ini(ini_file='criteria.ini')
#
#         region_name = getattr(feature_doc, 'region')
#         disease = getattr(feature_doc, 'disease')
#         region_id = feature_doc.doc_id()
# #         genes = feature_doc['genes']
# #         diseases = feature_doc['diseases']
# #         study_id = feature_doc['study_id']
# #         author = feature_doc['authors'][0]
# 
#         print('Region id and name ' + region_id + '  name ' + region_name + 'disease ' + disease)
#         criteria_dict = cls.get_criteria_dict(region_id, region_name)
# 
#         dis_dict = dict()
#         criteria_disease_dict = {}
# 
#         dis_dict[disease] = []
#         if len(result_container_.get(gene, {})) > 0:
# 
#             criteria_disease_dict = result_container_[gene]
#             criteria_disease_dict = cls.get_criteria_disease_dict(diseases, criteria_dict,
#                                                                   criteria_disease_dict)
# 
#             result_container_[gene] = criteria_disease_dict
#         else:
#             criteria_disease_dict = {}
#             criteria_disease_dict = cls.get_criteria_disease_dict(diseases, criteria_dict,
#                                                                   criteria_disease_dict)
#             result_container_[gene] = criteria_disease_dict
# 
#         return result_container_

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
                region_name = getattr(doc, 'region')
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

