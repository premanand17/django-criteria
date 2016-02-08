import logging
from builtins import classmethod
from criteria.helper.criteria import Criteria
from elastic.search import ElasticQuery, Search
from elastic.elastic_settings import ElasticSettings
from elastic.query import Query


logger = logging.getLogger(__name__)


class RegionCriteria(Criteria):
    global counter
    counter = 1

    ''' RegionCriteria class define functions for building marker index type within criteria index

    '''
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
    def is_region_in_mhc(cls, hit, section=None, config=None, result_container={}):

        feature_id = hit['_id']
        result_container_ = cls.tag_feature_to_all_diseases(feature_id, section, config, result_container)
        return result_container_

    @classmethod
    def is_region_for_disease(cls, hit, section=None, config=None, result_container={}):

        feature_doc = hit['_source']
        feature_doc['_id'] = hit['_id']
        disease_loci = feature_doc['disease_loci']
        region_name = feature_doc['region_name']
        region_id = feature_doc['region_id']

        diseases = set()
        for disease_locus_id in disease_loci:

            query = ElasticQuery(Query.ids([disease_locus_id]), sources=['hits'])
            elastic = Search(query, idx=ElasticSettings.idx('REGION', idx_type='DISEASE_LOCUS'))
            disease_locus_hits = elastic.search().docs

            for disease_locus_hit in disease_locus_hits:
                hits = getattr(disease_locus_hit, 'hits')
                for hit in hits:
                    query = ElasticQuery(Query.ids([hit]))
                    elastic = Search(query, idx=ElasticSettings.idx('REGION', idx_type='STUDY_HITS'))
                    hit_doc = elastic.search().docs[0]

                    disease = getattr(hit_doc, "disease")
                    status = getattr(hit_doc, "status")

                    if status != 'N':
                        return result_container

                    disease_loci = getattr(hit_doc, "disease_locus").lower()

                    if disease_loci == 'tbc':
                        return result_container

                    diseases.add(disease)

        result_container_populated = cls.populate_container(region_id,
                                                            region_name,
                                                            fnotes=None, features=[region_id],
                                                            diseases=list(diseases),
                                                            result_container=result_container)

        return result_container_populated

    @classmethod
    def get_disease_tags(cls, feature_id):

        idx = ElasticSettings.idx('REGION_CRITERIA')
        docs = Criteria.get_disease_tags(feature_id, idx)
        return docs
