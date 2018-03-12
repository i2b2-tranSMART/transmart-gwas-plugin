package com.recomdata.grails.plugin.gwas

import org.transmart.biomart.BioAssayAnalysisDataIdx

class GwasSearchService {

	static transactional = false

	List<Object[]> getGwasData(analysisId) {
		getGwasData(analysisId, null)
	}

	List<Object[]> getGwasData(analysisId, ranges) {
		String hql = '''
				SELECT gwas.rsId, gwas.pValue, gwas.logPValue, gwas.ext_data
				FROM	BioAssayAnalysisGwas gwas
				WHERE	gwas.analysis.id = :parAnalysisId'''
		if (ranges) {
			hql += ' AND gwas.rsId IN (:parSearchProbes)'
		}

		BioAssayAnalysisGwas.executeQuery(hql, [parAnalysisId: analysisId], [max: 100])
	}

	List<BioAssayAnalysisDataIdx> getGwasIndexData() {
		BioAssayAnalysisDataIdx.findAllByExt_type('GWAS', [sort: 'display_idx', order: 'asc'])
	}

	List<BioAssayAnalysisDataIdx> getEqtlIndexData() {
		BioAssayAnalysisDataIdx.findAllByExt_type('EQTL', [sort: 'display_idx', order: 'asc'])
	}

	List<Object[]> getEqtlData(analysisId) {
		getEqtlData(analysisId, null)
	}

	List<Object[]> getEqtlData(analysisId, searchProbes) {
		BioAssayAnalysisEqtl.executeQuery('''
			SELECT eqtl.rsId,eqtl.pValue,eqtl.logPValue,eqtl.ext_data
			FROM	BioAssayAnalysisEqtl eqtl
			WHERE	eqtl.analysis.id = :parAnalaysisId
			''', [parAnalaysisId: analysisId], [max: 100])
	}
}
