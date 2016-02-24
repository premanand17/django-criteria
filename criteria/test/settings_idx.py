from elastic.elastic_settings import ElasticSettings

OVERRIDE_SETTINGS = \
    {
     'default': {
        'ELASTIC_URL': ElasticSettings.url(),
        'IDX': {
            'GENE_CRITERIA': {
                'name': 'pydgin_imb_criteria_gene_test',
                'idx_type': {
                    'IS_GENE_IN_MHC': {'type': 'is_gene_in_mhc',  'auth_public': True},
                    'CAND_GENE_IN_STUDY': {'type': 'cand_gene_in_study',  'auth_public': True},
                    'CAND_GENE_IN_REGION': {'type': 'cand_gene_in_region',  'auth_public': True},
                    'GENE_IN_REGION': {'type': 'gene_in_region',  'auth_public': True},
                },
                'auth_public': True
            },
            'MARKER_CRITERIA': {
                'name': 'pydgin_imb_criteria_marker_test',
                'idx_type': {
                    'IS_MARKER_IN_MHC': {'type': 'is_marker_in_mhc',  'auth_public': True},
                    'IS_AN_INDEX_SNP': {'type': 'is_an_index_snp',  'auth_public': True},
                    'MARKER_IS_GWAS_SIGNIFICANT': {'type': 'marker_is_gwas_significant',  'auth_public': True},
                    'RSQ_WITH_INDEX_SNP': {'type': 'rsq_with_index_snp',  'auth_public': True},
                },
                'auth_public': True
            },
            'REGION_CRITERIA': {
                'name': 'pydgin_imb_criteria_region_test',
                'idx_type': {
                    'IS_REGION_IN_MHC': {'type': 'is_region_in_mhc',  'auth_public': True},
                    'IS_REGION_FOR_DISEASE': {'type': 'is_region_for_disease',  'auth_public': True},
                },
                'auth_public': True
            },
            'STUDY_CRITERIA': {
                'name': 'pydgin_imb_criteria_study_test',
                'idx_type': {
                    'STUDY_FOR_DISEASE': {'type': 'study_for_disease',  'auth_public': True},
                },
                'auth_public': True
            },

        },
        'TEST': 'auto_tests',
        'REPOSITORY': 'my_backup',
        'TEST_REPO_DIR': '/ipswich/data/pydgin/elastic/repos/',
     }
    }
