package com.recomdata.grails.plugin.gwas

import grails.test.mixin.TestFor
import spock.lang.Specification

/**
 * @author <a href='mailto:burt_beckwith@hms.harvard.edu'>Burt Beckwith</a>
 */
@TestFor(GwasWebController)
class GwasWebControllerSpec extends Specification {

	private static final List<List> RESULTS = [['a', 2, 'tree']]
	private static final String RESULT_XML = '<rows><row><data>a</data><data>2</data><data>tree</data></row></rows>'

	void setupSpec() {
		defineBeans {
			gwasWebService GwasWebService
		}
		controller.gwasWebService = applicationContext.gwasWebService
	}

	void 'test computeGeneBounds'() {
		when:
		controller.gwasWebService = new GwasWebService() {
			List<List> computeGeneBounds(String geneSymbol, String snpSource) {
				RESULTS
			}
		}

		controller.computeGeneBounds()

		then:
		response.text == RESULT_XML
	}

	void 'test getGeneByPosition'() {
		when:
		controller.gwasWebService = new GwasWebService() {
			List<List> getGeneByPosition(String chromosome, Long start, Long stop, String snpSource) {
				RESULTS
			}
		}

		controller.getGeneByPosition()

		then:
		response.text == RESULT_XML
	}

	void 'test getModelInfoByDataType'() {
		when:
		controller.gwasWebService = new GwasWebService() {
			List<List> getModelInfo(String type) {
				RESULTS
			}
		}

		controller.getModelInfoByDataType()

		then:
		response.text == RESULT_XML
	}

	void 'test getSecureModelInfoByDataType'() {
		when:
		controller.gwasWebService = new GwasWebService() {
			List<List> getSecureModelInfo(String type) {
				RESULTS
			}
		}

		controller.getSecureModelInfoByDataType()

		then:
		response.text == RESULT_XML
	}

	void 'test resultDataForFilteredByModelIdGeneAndRangeRev'() {
		when:
		controller.gwasWebService = new GwasWebService() {
			List<List> computeGeneBounds(String geneSymbol, String snpSource) {
				[[0, 42, 'abc']]
			}

			List<List> getAnalysisDataBetween(String[] analysisIds, long low, long high, String chrom, String snpSource) {
				RESULTS
			}
		}

		params.modelId = '1'
		controller.resultDataForFilteredByModelIdGeneAndRangeRev()

		then:
		response.text == RESULT_XML
	}

	void 'test getSnpSources'() {
		when:
		controller.getSnpSources()

		then:
		response.text == '<rows>' +
				'<row>' +
				'<data>18</data>' +
				'<data>HG18</data>' +
				'<data>18</data>' +
				'<data>03-2006</data>' +
				'<data>http://www.example.com</data>' +
				'</row>' +
				'<row>' +
				'<data>19</data>' +
				'<data>HG19</data>' +
				'<data>19</data>' +
				'<data>02-2009</data>' +
				'<data>http://www.example.com</data>' +
				'</row>' +
				'</rows>'
	}

	void 'test getGeneSources'() {
		when:
		controller.getGeneSources()

		then:
		response.text == '<rows>' +
				'<row>' +
				'<data>0</data>' +
				'<data>GRCh37</data>' +
				'<data>0</data>' +
				'<data>01-2001</data>' +
				'<data>http://www.example.com</data>' +
				'</row>' +
				'</rows>'
	}

	void 'test getRecombinationRatesForGene'() {
		when:
		controller.gwasWebService = new GwasWebService() {
			List<List> getRecombinationRatesForGene(String geneSymbol, Long range) {
				RESULTS
			}
		}

		controller.getRecombinationRatesForGene()

		then:
		response.text == RESULT_XML
	}

	void 'test snpSearch'() {
		when:
		controller.gwasWebService = new GwasWebService() {
			List<List> snpSearch(Object analysisIds, Long range, String rsId, String hgVersion) {
				RESULTS
			}
		}

		params.modelId = '1'
		controller.snpSearch()

		then:
		response.text == RESULT_XML
	}

	void 'test recombinationRateBySnp'() {
		when:
		controller.gwasWebService = new GwasWebService() {
			List<List> getRecombinationRateBySnp(Object snp, Object range, Object hgVersion) {
				RESULTS
			}
		}

		controller.recombinationRateBySnp()

		then:
		response.text == RESULT_XML
	}
}
