''' Core DRF web-services. '''
from rest_framework import serializers, mixins
from criteria.rest_framework.feature_resources import ListCriteriaMixin
from elastic.rest_framework.resources import ElasticLimitOffsetPagination
from rest_framework.viewsets import GenericViewSet


class CriteriaSerializer(serializers.Serializer):
    ''' Serializer for criteria. '''
    qid = serializers.CharField(help_text='feature id', required=True)
    criteria_type = serializers.CharField(help_text='index_type', required=True)
    disease_tags = serializers.ListField(help_text='disease tags')
    feature_details = serializers.ListField(help_text='feature_details')


class CriteriaViewSet(ListCriteriaMixin, mixins.ListModelMixin, GenericViewSet):
    ''' Returns a list of Criteria documents.
    ---
    list:
        response_serializer: CriteriaSerializer
        parameters:
            - name: feature_type
              description: Feature type (e.g. GENE,MARKER,STUDY,REGION,ALL)
              required: false
              type: string
              defaultValue: 'GENE'
              enum: ['GENE', 'MARKER', 'STUDY', 'REGION', 'ALL']
              paramType: query
            - name: feature_id
              description: Feature id (e.g. ENSG00000134242,rs2476601)
              required: false
              type: string
              paramType: query
            - name: aggregate
              defaultValue: false
              enum: [true, false]
              description: aggregate disease_tags across all index types for a given feature_id
              type: boolean
              paramType: query
            - name: detail
              defaultValue: false
              enum: [true, false]
              description: feature details
              type: boolean
              paramType: query
    '''

    serializer_class = CriteriaSerializer
    pagination_class = ElasticLimitOffsetPagination
    filter_fields = ('feature_type', 'feature_id', 'aggregate', 'detail')
