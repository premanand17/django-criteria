
==========================
Criteria (django-criteria)
==========================
Plugin to manage creation and management of criteria index

Search is a Django app to run Elastic search queries.

Quick start
-----------

1. Installation::

    pip install -e git://github.com/D-I-L/django-criteria.git#egg=criteria
    

2. If you need to start a Django project::

    django-admin startproject [project_name]

3. Add "criteria" to your ``INSTALLED_APPS`` in ``settings.py``::

    INSTALLED_APPS = (
        ...
        'criteria',
    )

4. Update the Elastic settings in the settings.py::

	  'GENE_CRITERIA': {
	                'name': 'pydgin_imb_criteria_gene',
	                'idx_type': {
	                    'IS_GENE_IN_MHC': {'type': 'is_gene_in_mhc',  'auth_public': True},
	                    'CAND_GENE_IN_STUDY': {'type': 'cand_gene_in_study',  'auth_public': True},
	                    'CAND_GENE_IN_REGION': {'type': 'cand_gene_in_region',  'auth_public': True},
	                    'GENE_IN_REGION': {'type': 'gene_in_region',  'auth_public': True},
	                },
	                'auth_public': True
	            },
	            'MARKER_CRITERIA': {
	                'name': 'pydgin_imb_criteria_marker',
	                'idx_type': {
	                    'IS_MARKER_IN_MHC': {'type': 'is_marker_in_mhc',  'auth_public': True},
	                    'IS_AN_INDEX_SNP': {'type': 'is_an_index_snp',  'auth_public': True},
	                    'MARKER_IS_GWAS_SIGNIFICANT_STUDY': {'type': 'marker_is_gwas_significant_in_study',  'auth_public': True},
	                    'RSQ_WITH_INDEX_SNP': {'type': 'rsq_with_index_snp',  'auth_public': True},
	                    'MARKER_IS_GWAS_SIGNIFICANT_IC': {'type': 'marker_is_gwas_significant_in_ic',  'auth_public': True},
	                },
	                'auth_public': True
	            },
	            'REGION_CRITERIA': {
	                'name': 'pydgin_imb_criteria_region',
	                'idx_type': {
	                    'IS_REGION_IN_MHC': {'type': 'is_region_in_mhc',  'auth_public': True},
	                    'IS_REGION_FOR_DISEASE': {'type': 'is_region_for_disease',  'auth_public': True},
	                },
	                'auth_public': True
	            },
	            'STUDY_CRITERIA': {
	                'name': 'pydgin_imb_criteria_study',
	                'idx_type': {
	                    'STUDY_FOR_DISEASE': {'type': 'study_for_disease',  'auth_public': True},
	                },
	                'auth_public': True
	            },
 
 5. Tests can be run as follows::

    	./manage.py test criteria.test
  
============================================
Create Mapping and Loading Data into Elastic
============================================
	All the criterias are defined in criteria.ini, each section for one criteria.  Criteria indexes are derived from the source indexes. So make sure that the source indexes 
	exists and you are pointing to the right source index keys as defined in elastic_settings.
	
	[cand_gene_in_study]
	desc: Candidate Gene for a Study
	feature: gene
	link_to_feature: study
	source_idx: STUDY
	source_idx_type: STUDY
	source_fields : study_id,genes,diseases,authors
	test_id: ENSG00000136634
	text:A <strong>candidate gene in a study</strong> is defined as a gene cited in the principal paper of one of our curated studies.  Following the link will take you to the study.

  Help:
  	./manage.py criteria_index --help
  
  Show (list the criterias, without creating them):
	  ./manage.py criteria_index --feature gene --show
	  (Output: ['is_gene_in_mhc', 'cand_gene_in_study', 'cand_gene_in_region', 'gene_in_region'])
  
	  ./manage.py criteria_index --feature region --show
	  (Output: ['is_region_in_mhc', 'is_region_for_disease'])
	  
	  ./manage.py criteria_index --feature marker --show
	  (Output: ['is_marker_in_mhc', 'is_an_index_snp', 'marker_is_gwas_significant_in_study', 'marker_is_gwas_significant_in_ic', 'rsq_with_index_snp'])
	  
	  ./manage.py criteria_index --feature region --show
	  (Output: ['is_region_in_mhc', 'is_region_for_disease'])
  
 Run all criterias for feature gene (in normal mode):
  	./manage.py criteria_index --feature gene
  
 Run all criterias for feature gene (in test mode):
  	./manage.py criteria_index --feature gene --test
  
 Run one criteria for feature gene:
  	./manage.py criteria_index --feature gene --criteria cand_gene_in_study
  
 Run one criteria for feature marker:
  	./manage.py criteria_index --feature marker --criteria is_an_index_snp
  
  
  
  
 