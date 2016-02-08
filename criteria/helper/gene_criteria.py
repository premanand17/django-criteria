import logging
from builtins import classmethod
from elastic.search import ElasticQuery, Search
from elastic.query import Query
from elastic.elastic_settings import ElasticSettings
from criteria.helper.criteria import Criteria
from region import utils
from elastic.result import Document
from elastic.aggs import Agg, Aggs
from criteria.helper.criteria_manager import CriteriaManager

logger = logging.getLogger(__name__)


class GeneCriteria(Criteria):

    ''' GeneCriteria class define functions for building gene index type within criteria index

    '''

    @classmethod
    def cand_gene_in_study(cls, hit, section=None, config=None, result_container={}):

        result_container_ = result_container
        feature_doc = hit['_source']
        feature_doc['_id'] = hit['_id']

        genes = feature_doc['genes']
        diseases = feature_doc['diseases']
        study_id = feature_doc['study_id']
        author = feature_doc['authors'][0]

        first_author = author['name'] + ' ' + author['initials']

        result_container_populated = cls.populate_container(study_id,
                                                            first_author,
                                                            fnotes=None, features=genes,
                                                            diseases=diseases,
                                                            result_container=result_container_)
        return result_container_populated

    @classmethod
    def cand_gene_in_region(cls, hit, section=None, config=None, result_container={}):

        feature_doc = hit['_source']
        feature_doc['_id'] = hit['_id']

        genes = []
        if 'genes' in feature_doc:
            genes = feature_doc['genes']

        disease = None
        if 'disease' in feature_doc:
            disease = feature_doc['disease']

        status = None
        if 'status' in feature_doc:
            status = feature_doc['status']

        if genes is None or disease is None or status is None:
            return result_container

        if status != 'N':
            return result_container

        disease_loci = feature_doc["disease_locus"].lower()

        if disease_loci == 'tbc':
            return result_container

        region_index = ElasticSettings.idx('REGION', idx_type='STUDY_HITS')
        (region_idx, region_idx_type) = region_index.split('/')

        print(region_idx + '  ' + region_idx_type)

        gene_dict = cls.get_gene_docs_by_ensembl_id(genes, sources=['chromosome', 'start', 'stop'])

        for gene in gene_dict:
            # get position
            gene_doc = gene_dict[gene]
            print(gene_doc.__dict__)
            build = "38"  # get it from index name genes_hg38_v0.0.2 TODO
            seqid = getattr(gene_doc, "chromosome")
            start = getattr(gene_doc, "start")
            stop = getattr(gene_doc, "stop")
            # check if they overlap a region
            overlapping_region_docs = cls.fetch_overlapping_features(build, seqid, start, stop,
                                                                     idx=region_idx, idx_type=region_idx_type)

            region_docs = utils.Region.hits_to_regions(overlapping_region_docs)

            if(region_docs is None or len(region_docs) == 0):
                continue

            for region_doc in region_docs:
                print(region_doc.__dict__)
                region_id = getattr(region_doc, "region_id")
                region_name = getattr(region_doc, "region_name")

                result_container_populated = cls.populate_container(region_id,
                                                                    region_name,
                                                                    fnotes=None, features=[gene],
                                                                    diseases=[disease],
                                                                    result_container=result_container)
                result_container = result_container_populated

        return result_container

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
    def is_gene_in_mhc(cls, hit, section=None, config=None, result_container={}):

        feature_id = hit['_id']
        result_container_ = cls.tag_feature_to_all_diseases(feature_id, section, config, result_container)
        return result_container_

    @classmethod
    def gene_in_region(cls, hit, section=None, config=None, result_container={}):

        try:
            padded_region_doc = utils.Region.pad_region_doc(Document(hit))
        except:
            logger.warn('Region padding error ')
            return result_container

        # 'build_info': {'end': 22411939, 'seqid': '1', 'build': 38, 'start': 22326008}, 'region_id': '1p36.12_008'}
        region_id = getattr(padded_region_doc, "region_id")
        region_name = getattr(padded_region_doc, "region_name")
        build_info = getattr(padded_region_doc, "build_info")
        diseases = getattr(padded_region_doc, "tags")['disease']
        seqid = build_info['seqid']
        start = build_info['start']
        end = build_info['end']

        gene_index = ElasticSettings.idx('GENE', idx_type='GENE')
        elastic = Search.range_overlap_query(seqid=seqid, start_range=start, end_range=end,
                                             idx=gene_index, field_list=['start', 'stop', '_id'],
                                             seqid_param="chromosome",
                                             end_param="stop")
        result_docs = elastic.search().docs

        genes = set()
        for doc in result_docs:
            genes.add(doc.doc_id())

        result_container_populated = cls.populate_container(region_id,
                                                            region_name,
                                                            fnotes=None, features=genes,
                                                            diseases=diseases,
                                                            result_container=result_container)
        return result_container_populated

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
    def get_gene_docs_by_ensembl_id(cls, ens_ids, sources=None):
        ''' Get the gene symbols for the corresponding array of ensembl IDs.
        A dictionary is returned with the key being the ensembl ID and the
        value the gene document. '''
        query = ElasticQuery(Query.ids(ens_ids), sources=sources)
        elastic = Search(query, idx=ElasticSettings.idx('GENE', idx_type='GENE'), size=len(ens_ids))
        return {doc.doc_id(): doc for doc in elastic.search().docs}

    @classmethod
    def get_disease_tags(cls, feature_id):

        idx = ElasticSettings.idx('GENE_CRITERIA')
        docs = Criteria.get_disease_tags(feature_id, idx)
        return docs
