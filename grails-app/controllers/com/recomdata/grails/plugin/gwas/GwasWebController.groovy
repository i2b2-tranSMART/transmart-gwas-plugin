/*************************************************************************
 * tranSMART - translational medicine data mart
 *
 * Copyright 2008-2012 Janssen Research & Development, LLC.
 *
 * This product includes software developed at Janssen Research & Development, LLC.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software  * Foundation, either version 3 of the License, or (at your option) any later version, along with the following terms:
 * 1.	You may convey a work based on this program in accordance with section 5, provided that you retain the above notices.
 * 2.	You may convey verbatim copies of this program code as you receive it, in any medium, provided that you retain the above notices.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS    * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *
 ******************************************************************/

package com.recomdata.grails.plugin.gwas

class GwasWebController {

	private static final Map<Long, String> typeIds = [1L: 'GWAS', 2L: 'EQTL', 3L: 'Metabolic GWAS'].asImmutable()

	GwasWebService gwasWebService

	def computeGeneBounds(String snpSource, String geneSymbol) {
		renderDataSet gwasWebService.computeGeneBounds(geneSymbol, '0', snpSource ?: '19')
	}

	def getGeneByPosition(String snpSource, String chromosome, Long start, Long stop) {
		renderDataSet gwasWebService.getGeneByPosition(chromosome, start, stop, snpSource ?: '19')
	}

	def getModelInfoByDataType(Long dataType) {
		renderDataSet gwasWebService.getModelInfo(typeIds[dataType] ?: 'NONE')
	}

	def getSecureModelInfoByDataType(String user, Long dataType) {
		String type = typeIds[dataType] ?: 'NONE'
		Map sessionUserMap = servletContext.gwasSessionUserMap
		if (sessionUserMap == null) {
			sessionUserMap = [:]
			servletContext.gwasSessionUserMap = sessionUserMap
		}

		renderDataSet gwasWebService.getSecureModelInfo(type, sessionUserMap[user])
	}

	//TODO Negotiate this name into something more reasonable
	def resultDataForFilteredByModelIdGeneAndRangeRev(String snpSource, String modelId, String geneName) {
		if (!snpSource) {
			snpSource = '19'
		}
		long range = params.long('range') ?: 0

		def geneBounds = gwasWebService.computeGeneBounds(geneName, '0', snpSource)
		def low = geneBounds[0]
		def high = geneBounds[1]
		def chrom = geneBounds[2]

		renderDataSet gwasWebService.getAnalysisDataBetween(modelId.split(','), low - range, high + range, chrom, snpSource)
	}

	def getSnpSources() {
		renderDataSet([[18, 'HG18', 18, '03-2006', 'http://www.example.com'], [19, 'HG19', 19, '02-2009', 'http://www.example.com']])
	}

	def getGeneSources() {
		renderDataSet([[0, 'GRCh37', 0, '01-2001', 'http://www.example.com']])
	}

	def getRecombinationRatesForGene(Long range, String geneName) {
		renderDataSet gwasWebService.getRecombinationRatesForGene(geneName, range ?: 0)
	}

	def snpSearch(Long range, String modelId, String snp, String snpSource) {
		renderDataSet gwasWebService.snpSearch(modelId.split(','), range ?: 0, snp, snpSource ?: '19')
	}

	def recombinationRateBySnp(Long range, String snp, String snpSource) {
		renderDataSet gwasWebService.getRecombinationRateBySnp(snp, range ?: 0, snpSource ?: '19')
	}

	def renderDataSet(results) {
		render(contentType: 'text/xml', encoding: 'UTF-8') {
			rows {
				for (result in results) {
					row {
						for (dat in result) {
							data dat
						}
					}
				}
			}
		}
	}
}
