import logging
from builtins import classmethod
from region import utils
from elastic.result import Document
from criteria.helper.criteria import Criteria
from django.conf import settings
import pyRserve
from elastic.query import BoolQuery, Query
from elastic.search import ElasticQuery, Search
from elastic.elastic_settings import ElasticSettings
import json
from criteria.helper.criteria_manager import CriteriaManager

logger = logging.getLogger(__name__)


class MarkerCriteria(Criteria):
    global counter
    counter = 1

    ''' MarkerCriteria class define functions for building marker criterias, each as separate index types
    '''

    FEATURE_TYPE = 'marker'

    @classmethod
    def is_an_index_snp(cls, hit, section=None, config=None, result_container={}):

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

        region_docs = utils.Region.hits_to_regions([Document(hit)])

        for region_doc in region_docs:
                region_id = getattr(region_doc, "region_id")
                region_name = getattr(region_doc, "region_name")

                result_container_populated = cls.populate_container(region_id,
                                                                    region_name,
                                                                    fnotes=None, features=[marker],
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
    def is_marker_in_mhc(cls, hit, section=None, config=None, result_container={}):
        global counter
        feature_id = hit['_source']['id']
        print(hit)
        result_container_ = cls.tag_feature_to_all_diseases(feature_id, section, config, result_container)
        print(str(counter) + ' Feature id ' + feature_id)
        counter = counter + 1
        return result_container_

    @classmethod
    def rsq_with_index_snp(cls, hit, section=None, config=None, result_container={}):
        feature_doc = hit['_source']
        feature_doc['_id'] = hit['_id']

        marker1 = None
        if 'marker' in feature_doc:
            marker1 = feature_doc['marker']

        disease = None
        if 'disease' in feature_doc:
            disease = feature_doc['disease']

        status = None
        if 'status' in feature_doc:
            status = feature_doc['status']

        if marker1 is None or disease is None or status is None:
            return result_container

        if status != 'N':
            return result_container

        disease_loci = feature_doc["disease_locus"].lower()

        if disease_loci == 'tbc':
            return result_container

        dil_study_id = feature_doc["dil_study_id"]

        global counter
        counter = counter + 1

        # get the markers that is in ld with the above marker and add it as fid, fname
        # http://tim-rh3:8000/rest/ld/?build=GRCh38&dataset=--EUR--&dprime=0&format=json&m1=rs6679677&rsq=0.8
        # query marker1 over the marker index to get the seqid and call the ld_run wiht right parameters
        # for the marker2 that is in ld with marker1, tag it with the right disease and studyid
        # query study index with the above dil_study_id to get the author name

        query = ElasticQuery(BoolQuery(must_arr=[Query.term("id", marker1)]), sources=['seqid', 'start'])
        elastic = Search(search_query=query, idx=ElasticSettings.idx('MARKER', 'MARKER'), size=1)
        docs = elastic.search().docs
        marker_doc = None

        if docs is not None and len(docs) > 0:
            marker_doc = elastic.search().docs[0]

        if marker_doc is None:
            return result_container

        seqid = getattr(marker_doc, 'seqid')

        rserve = getattr(settings, 'RSERVE')

        conn = pyRserve.connect(host=rserve.get('HOST'), port=rserve.get('PORT'))
        dataset = 'EUR'
        rsq = 0.8

        ld_str = conn.r.ld_run(dataset, seqid, marker1,
                               dprime=0, rsq=rsq)
        ld_str = ld_str.replace('D.prime', 'dprime').replace('R.squared', 'rsquared')
        conn.close()
        ld = json.loads(str(ld_str))

        if 'error' in ld:
            return result_container

        marker_list = ld['ld']

        if marker_list is None or len(marker_list) == 0:
            return result_container

        query = ElasticQuery(Query.ids([dil_study_id]))
        elastic = Search(search_query=query, idx=ElasticSettings.idx('STUDY', 'STUDY'), size=1)
        study_doc = elastic.search().docs[0]
        author = getattr(study_doc, 'authors')[0]
        first_author = author['name'] + ' ' + author['initials']

        for marker_dict in marker_list:

            marker2 = marker_dict['marker2']
            rsquared = marker_dict['rsquared']

            marker_id = marker1
            marker_name = marker1

            fnotes = {'linkdata': 'rsq', 'linkvalue': rsquared, 'linkid': dil_study_id, 'linkname': first_author}

            result_container_populated = cls.populate_container(marker_id,
                                                                marker_name,
                                                                fnotes=fnotes, features=[marker2],
                                                                diseases=[disease],
                                                                result_container=result_container)
            result_container = result_container_populated

        return result_container

    @classmethod
    def marker_is_gwas_significant(cls, hit, section=None, config=None, result_container={}):
        gw_sig_p = 0.00000005
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

        dil_study_id = feature_doc["dil_study_id"]

        p_val_to_compare = None
        combined_p_val = feature_doc["p_values"]["combined"]
        discovery_p_val = feature_doc["p_values"]["discovery"]
        replication_p_val = feature_doc["p_values"]["replication"]

        if combined_p_val is not None:
            p_val_to_compare = combined_p_val

        if p_val_to_compare is None:
            p_val_to_compare = discovery_p_val

        if p_val_to_compare is None:
            p_val_to_compare = replication_p_val

        if p_val_to_compare is None:
            return result_container

        global counter
        counter = counter + 1

        p_val_to_compare = float(p_val_to_compare)
        if p_val_to_compare < gw_sig_p:
            query = ElasticQuery(Query.ids([dil_study_id]))
            elastic = Search(search_query=query, idx=ElasticSettings.idx('STUDY', 'STUDY'), size=1)
            study_doc = elastic.search().docs[0]
            author = getattr(study_doc, 'authors')[0]
            first_author = author['name'] + ' ' + author['initials']
            fnotes = {'linkdata': 'pval', 'linkvalue': p_val_to_compare,
                      'linkid': dil_study_id, 'linkname': first_author}
            result_container_populated = cls.populate_container(dil_study_id,
                                                                first_author,
                                                                fnotes=fnotes, features=[marker],
                                                                diseases=[disease],
                                                                result_container=result_container)
            return result_container_populated
        else:
            return result_container

    @classmethod
    def get_disease_tags(cls, feature_id):

        idx = ElasticSettings.idx('MARKER_CRITERIA')
        docs = Criteria.get_disease_tags(feature_id, idx)
        return docs

    @classmethod
    def get_available_criterias(cls, feature=None, config=None):
        'Function to get available criterias for marker'
        if config is None:
            config = CriteriaManager.get_criteria_config()

        if feature is None:
            feature = cls.FEATURE_TYPE

        available_criterias = Criteria.get_available_criterias(feature, config)
        return available_criterias

    @classmethod
    def get_criteria_details(cls, feature_id, idx=None, idx_type=None, config=None):

        # get all the criterias from ini
        if idx_type is None:
            available_criterias = cls.get_available_criterias(feature=cls.FEATURE_TYPE, config=config)
            criteria_list = available_criterias[cls.FEATURE_TYPE]
            idx_type = ','.join(criteria_list)

        if idx is None:
            idx = ElasticSettings.idx('MARKER_CRITERIA')

        result_dict = Criteria.get_criteria_details(feature_id, idx, idx_type)
        return result_dict
