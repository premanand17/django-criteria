import logging
from builtins import classmethod
from elastic.search import ElasticQuery, Search
from elastic.query import Query, BoolQuery
from elastic.elastic_settings import ElasticSettings
from criteria.helper.criteria import Criteria
from region import utils
from elastic.result import Document
from criteria.helper.criteria_manager import CriteriaManager

logger = logging.getLogger(__name__)


class GeneCriteria(Criteria):

    ''' GeneCriteria class define functions for building gene criterias, each as separate index types

    '''
    FEATURE_TYPE = 'gene'

    @classmethod
    def cand_gene_in_study(cls, hit, section=None, config=None, result_container={}):
        '''function that implements the cand_gene_in_study criteria
        '''

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
        '''function that implements the cand_gene_in_region criteria
        '''
        feature_doc = hit['_source']
        feature_doc['_id'] = hit['_id']

        disease_loci = feature_doc["disease_locus"].lower()

        if disease_loci == 'tbc':
            return result_container

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

        region_index = ElasticSettings.idx('REGION', idx_type='STUDY_HITS')
        (region_idx, region_idx_type) = region_index.split('/')

        # print(region_idx + '  ' + region_idx_type)

        gene_dict = cls.get_gene_docs_by_ensembl_id(genes, sources=['chromosome', 'start', 'stop'])

        for gene in gene_dict:
            # get position
            gene_doc = gene_dict[gene]
            # print(gene_doc.__dict__)
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
                                             end_param="stop", size=10000)
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
    def exonic_index_snp_in_gene(cls, hit, section=None, config=None, result_container={}):

        feature_doc = hit['_source']
        feature_doc['_id'] = hit['_id']

        marker = None
        if 'marker' in feature_doc:
            marker = feature_doc['marker']

        disease = None
        if 'disease' in feature_doc:
            disease = feature_doc['disease']

        status = None
        if 'status' in feature_doc:
            status = feature_doc['status']

        if marker is None or disease is None or status is None:
            return result_container

        if status != 'N':
            return result_container

        disease_loci = feature_doc["disease_locus"].lower()

        if disease_loci == 'tbc':
            return result_container

        # get marker info and gene info from function info dbsp
        # get marker doc
        query = ElasticQuery(BoolQuery(must_arr=[Query.term("id", marker)]), sources=['id', 'info'])
        elastic = Search(search_query=query, idx=ElasticSettings.idx('MARKER', 'MARKER'), size=1)
        docs = elastic.search().docs
        marker_doc = None

        if docs is not None and len(docs) > 0:
            marker_doc = elastic.search().docs[0]

        if marker_doc is None:
            return result_container

        from marker.templatetags.marker_tags import marker_functional_info
        from marker.templatetags.marker_tags import gene_info

        ''' Retrieve functional information from bitfield in the INFO column.
        ftp://ftp.ncbi.nlm.nih.gov/snp/specs/dbSNP_BitField_latest.pdf

        ('has synonymous', True), ('has reference', True), ('has stop gain', False),
        ('has non-synonymous missense', False), ('has non-synonymous frameshift', False), ('has stop loss', False)])
        '''
        functional_info = marker_functional_info(marker_doc)
        # print(functional_info)

        is_in_exon = False

        if functional_info['has non-synonymous missense'] or\
            functional_info['has synonymous'] or functional_info['has non-synonymous frameshift'] or\
            functional_info['has reference'] or functional_info['has stop gain'] or\
                functional_info['has stop loss']:
                is_in_exon = True

        # gene_symbols = []
        if is_in_exon:
            gene_ids = gene_info(marker_doc)
            ensembl_gene_ids = gene_ids.values()
#             gene_symbols.extend(list(gene_ids.keys()))
#             print(ensembl_gene_ids)
#             for gene in gene_symbols:
#                 print('^^^\t'+gene)
        else:
            return result_container

        dil_study_id = feature_doc['dil_study_id']
        fnotes = None
        if dil_study_id:
            query = ElasticQuery(Query.ids([dil_study_id]))
            elastic = Search(search_query=query, idx=ElasticSettings.idx('STUDY', 'STUDY'), size=1)
            study_doc = elastic.search().docs[0]
            author = getattr(study_doc, 'authors')[0]
            first_author = author['name'] + ' ' + author['initials']
            fnotes = {'linkid': dil_study_id, 'linkname': first_author}

        result_container_populated = cls.populate_container(marker,
                                                            marker,
                                                            fnotes=fnotes, features=ensembl_gene_ids,
                                                            diseases=[disease],
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
    def get_disease_tags(cls, feature_id, idx_type=None):
        'Function to get disease tags for a given feature_id...delegated to parent class Criteria. Returns disease docs'
        idx = ElasticSettings.idx(cls.FEATURE_TYPE.upper()+'_CRITERIA')
        docs = Criteria.get_disease_tags(feature_id, idx, idx_type)
        return docs

    @classmethod
    def get_disease_tags_as_codes(cls, feature_id):
        '''Function to get disease tags for a given feature_id...delegated to parent class Criteria
        Returns disease codes'''
        disease_docs = cls.get_disease_tags(feature_id)
        disease_codes = [getattr(disease_doc, 'code') for disease_doc in disease_docs]
        return disease_codes

    @classmethod
    def get_all_criteria_disease_tags(cls, qids, idx_type=None):

        (idx, idx_types) = cls.get_feature_idx_n_idxtypes(cls.FEATURE_TYPE)

        if idx_type is None:
            idx_type = idx_types

        criteria_disease_tags = Criteria.get_all_criteria_disease_tags(qids, idx, idx_type)
        return(criteria_disease_tags)

    @classmethod
    def get_disease_codes_from_results(cls, criteria_results):
        idx = ElasticSettings.idx(cls.FEATURE_TYPE.upper()+'_CRITERIA')
        codes = Criteria.get_disease_codes_from_results(idx, criteria_results)
        return sorted(codes)

    @classmethod
    def get_available_criterias(cls, feature=None, config=None):
        'Function to get available criterias for gene'
        if config is None:
            config = CriteriaManager.get_criteria_config()

        if feature is None:
            feature = cls.FEATURE_TYPE

        available_criterias = Criteria.get_available_criterias(feature, config)
        return available_criterias

    @classmethod
    def get_criteria_details(cls, feature_id, idx=None, idx_type=None, config=None):
        'Function to get the criteria details for a given feature_id'
        if idx is None:
            idx = ElasticSettings.idx(cls.FEATURE_TYPE.upper()+'_CRITERIA')

        # get all the criterias from ini
        criteria_list = []
        if idx_type is None:
            available_criterias = cls.get_available_criterias(feature=cls.FEATURE_TYPE, config=config)
            criteria_list = available_criterias[cls.FEATURE_TYPE]
            idx_type = ','.join(criteria_list)

        result_dict = Criteria.get_criteria_details(feature_id, idx, idx_type)
        result_dict_expanded = Criteria.add_meta_info(idx, criteria_list, result_dict)
        print(result_dict_expanded)
        return result_dict_expanded
