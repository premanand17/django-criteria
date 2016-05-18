from django.shortcuts import render
import re
import logging
from criteria.forms import CriteriaForm
from django.core.urlresolvers import reverse
import json
from elastic.query import BoolQuery, Query, Filter

from elastic.search import ElasticQuery, Search
from elastic.elastic_settings import ElasticSettings
from criteria.helper.gene_criteria import GeneCriteria
from django.conf import settings
from criteria.helper.criteria import Criteria


logger = logging.getLogger(__name__)

#
# def criteria_home(request):
#     '''renders login home page'''
#     return render(request, 'criteria_tool/criteria_home.html')


def criteria_home(request):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = CriteriaForm(request.POST)
        # check whether it's valid:
        if form.is_valid():
            # process the data in form.cleaned_data as required
            identifiers_str = form.cleaned_data['query']
            identifiers = re.split('\n|,', identifiers_str)
            identifiers = [identifier.rstrip() for identifier in identifiers]
            print(identifiers)

            criteria_disease_tags = Criteria.do_criteria_search(identifiers)

            return render(request, 'criteria_tool/criteria_home.html', {'form': form, 'show_result': True,
                                                                        'criteria_disease_tags': criteria_disease_tags,
                                                                        'CDN': settings.CDN})

    # if a GET (or any other method) we'll create a blank form
    else:
        form = CriteriaForm()

    return render(request, 'criteria_tool/criteria_home.html', {'form': form, 'show_query': True})


def gene_lookup(query_terms):

    print('===============')
    print(query_terms)
    terms = re.sub("[^\w]", " ",  query_terms)
    print(terms)
    print('===============')

    equery = BoolQuery(b_filter=Filter(Query.query_string(terms, fields=['symbol'])))
    search_query = ElasticQuery(equery, sources=['symbol'])
    (idx, idx_type) = ElasticSettings.idx('GENE', 'GENE').split('/')
    result = Search(search_query=search_query, size=10, idx=idx, idx_type=idx_type).search()
    ensembl_ids = None
    if result.hits_total > 0:
        ensembl_ids = [doc.doc_id() for doc in result.docs]

    return ensembl_ids
