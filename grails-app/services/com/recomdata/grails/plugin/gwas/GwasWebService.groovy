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

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.codehaus.groovy.grails.commons.GrailsApplication
import org.springframework.beans.factory.annotation.Value
import org.transmart.searchapp.AuthUser
import org.transmart.searchapp.AuthUserSecureAccess
import org.transmart.searchapp.SecureAccessLevel
import org.transmart.searchapp.SecureObject

import javax.sql.DataSource

@Slf4j('logger')
class GwasWebService {

	static transactional = false

	// the column delimiter in the method to write the data file below.
	private static final String valueDelimiter = '\t'

	DataSource dataSource
	GrailsApplication grailsApplication

	@Value('${RModules.tempFolderDirectory:}')
	private String tempFolderDirectory

	List computeGeneBounds(String geneSymbol, String geneSourceId, String snpSource) {
		def row = new Sql(dataSource).firstRow(geneLimitsSqlQueryByKeyword, geneSymbol, snpSource)
		if (row) {
			[row.low, row.high, row.chrom]
		}
	}

	def getGeneByPosition(String chromosome, Long start, Long stop, String snpSource) {
		def query = genePositionSqlQuery
		def geneQuery = geneLimitsSqlQueryByEntrez

		//Create objects we use to form JDBC connection.
		def con, stmt, rs = null
		def geneStmt, geneInfoStmt, geneRs, geneInfoRs = null

		//Grab the connection from the grails object.
		con = dataSource.getConnection()

		//Prepare the SQL statement.
		stmt = con.prepareStatement(query)
		stmt.setString(1, chromosome)
		stmt.setLong(2, start)
		stmt.setLong(3, stop)
		stmt.setString(4, snpSource)
		rs = stmt.executeQuery()

		def results = []

		geneStmt = con.prepareStatement(geneQuery)
		geneInfoStmt = con.prepareStatement(getGeneStrand)

		try {
			while (rs.next()) {

				def entrezGeneId = rs.getLong('ENTREZ_GENE_ID')
				geneInfoStmt.setString(1, entrezGeneId.toString())
				geneInfoRs = geneInfoStmt.executeQuery()
				def strand = 0
				try {
					if (geneInfoRs.next()) {
						strand = geneInfoRs.getString('STRAND')
					}
				}
				finally {
					geneInfoRs.close()
				}
				logger.debug 'Gene strand query:{}', geneInfoRs
				geneStmt.setString(1, entrezGeneId.toString())
				geneStmt.setString(2, snpSource)
				geneRs = geneStmt.executeQuery()
				try {
					if (geneRs.next()) {
						results.push([
								rs.getString('BIO_MARKER_ID'),
								'GRCh37',
								rs.getString('BIO_MARKER_NAME'),
								rs.getString('BIO_MARKER_DESCRIPTION'),
								geneRs.getString('CHROM'),
								geneRs.getLong('LOW'),
								geneRs.getLong('HIGH'),
								strand,
								0,
								rs.getLong('ENTREZ_GENE_ID')
						])
					}
				}
				finally {
					geneRs?.close()
				}
			}

			return results
		}
		finally {
			rs?.close()
			geneRs?.close()
			stmt?.close()
			geneStmt?.close()
			con?.close()
		}
	}

	List<List> getModelInfo(String type) {
		modelInfo type, null
	}

	List<List> getSecureModelInfo(String type, String user) {
		modelInfo type, user
	}

	private List<List> modelInfo(String type, String user) {
		List<List> results = []
		new Sql(dataSource).eachRow(modelInfoSqlQuery, [type]) { row ->
			String studyName = row.studyName
			if (user && checkSecureStudyAccess(user.toLowerCase(), studyName)) {
				results << [row.id, row.modelName, row.analysisName, studyName]
			}
		}
		results
	}

	boolean checkSecureStudyAccess(String username, String accession) {
		logger.debug 'checking security for the user: {}', username
		Map<String, Long> secObjs = getExperimentSecureStudyList()
		if (!secObjs.containsKey(accession)) {
			return true
		}

		getGWASAccess(accession, AuthUser.findByUsername(username)) != 'Locked'
	}

	String getGWASAccess(String studyId, AuthUser user) {

		for (role in user.authorities) {
			if (isAdminRole(role)) {
				return 'Admin' //just set everything to admin and return it all
			}
		}

		Map<String, String> tokens = getSecureTokensWithAccessForUser(user)
		if (tokens.containsKey('EXP:' + studyId)) { //null tokens are assumed to be unlocked
			return tokens['EXP:' + studyId]; //found access for this token so put in access level
		}

		'Locked'; //didn't find authorization for this token
	}

	//return access levels for the children of this path that have them
	Map<String, Long> getExperimentSecureStudyList() {
		Map<String, Long> t = [:]
		List<Object[]> results = SecureObject.executeQuery('''
				SELECT so.bioDataUniqueId, so.bioDataId
				FROM SecureObject so
				Where so.dataType='Experiment' ''')
		for (Object[] row in results) {
			String token = row[0].replaceFirst('EXP:', '')
			Long dataid = row[1]
			t[token] = dataid
		}
		t
	}

	//return access levels for the children of this path that have them
	Map<String, String> getSecureTokensWithAccessForUser(user) {
		Map<String, String> t = [:]
		List<Object[]> results = AuthUserSecureAccess.executeQuery('''
				SELECT DISTINCT ausa.accessLevel, so.bioDataUniqueId
				FROM AuthUserSecureAccess ausa
				JOIN ausa.accessLevel
				JOIN ausa.secureObject so
				WHERE ausa.authUser IS NULL
				   OR ausa.authUser.id = ?''', [user.id])
		for (Object[] row in results) {
			SecureAccessLevel accessLevel = row[0]
			String token = row[1]
			t[token] = accessLevel.accessLevelName
		}
		t['EXP:PUBLIC'] = 'OWN'
		return t
	}

	def isAdminRole(role) {
		role.authority == 'ROLE_ADMIN' || role.authority == 'ROLE_DATASET_EXPLORER_ADMIN'
	}

	//Get all data for the given analysisIds that falls between the limits
	List<List> getAnalysisDataBetween(analysisIds, low, high, chrom, snpSource) {
		List<List> results = []
		new Sql(dataSource).eachRow(analysisDataSqlQueryGwas + analysisIds.join(',') + ')', [low, high, String.valueOf(chrom), snpSource]) { row ->
			results << [row.rsid, row.resultid, row.analysisid, row.pvalue, row.logpvalue, row.studyname,
			            row.analysisname, row.datatype, row.posstart, row.chromosome, row.gene, row.intronexon,
			            row.recombinationrate, row.regulome]
		}
		results
	}

	List<List> getRecombinationRatesForGene(String geneSymbol, Long range) {
		List<List> results = []
		new Sql(dataSource).eachRow(getRecombinationRatesForGeneQuery, [range, range, range, geneSymbol]) { row ->
			results << [row.position, row.rate]
		}
		results
	}

	def snpSearch(analysisIds, Long range, String rsId, String hgVersion) {
		List<List> results = []
		new Sql(dataSource).eachRow(snpSearchQuery.replace('_analysisIds_', analysisIds.join(',')), [range, range, rsId, hgVersion, hgVersion]) { row ->
			results << [row.rs_id, row.chrom, row.pos, row.LOG_P_VALUE, row.analysis_name,
			            row.gene, row.exon_intron, row.recombination_rate, row.regulome_score]
		}
		results
	}

	List<List> getRecombinationRateBySnp(snp, range, hgVersion) {
		List<List> results = []
		new Sql(dataSource).eachRow(recombinationRateBySnpQuery, [range, range, snp, hgVersion]) { row ->
			results << [row.chromosome, row.position, row.rate, row.map]
		}
		results
	}

	String createTemporaryDirectory(String jobName) {
		try {
			String jobTmpDirectory = tempFolderDirectory + '/' + jobName + '/'
			new File(jobTmpDirectory + 'workingDirectory').mkdirs()
			return jobTmpDirectory
		}
		catch (e) {
			throw new Exception('Failed to create Temporary Directories. Please contact an administrator.', e)
		}
	}

	/*
	 * Writes a file based on a passed in array of arrays.
	 */
	String writeDataFile(tempDirectory, dataToWrite, fileName) {

		File outputFile = new File(tempDirectory, fileName)
		String filePath = outputFile.absolutePath
		BufferedWriter output = outputFile.newWriter(true)

		try {
			dataToWrite.each { data ->
				data.each {
					output.write(it.toString())
					output.write(valueDelimiter)
				}
				output.newLine()
			}
		}
		catch (e) {
			throw new Exception('Failed when writing data to file.', e)
		}
		finally {
			output?.flush()
			output?.close()
		}

		filePath
	}

	private static final String geneLimitsSqlQueryByKeyword = '''
		SELECT max(snpinfo.pos) as high, min(snpinfo.pos) as low, min(snpinfo.chrom) as chrom
		FROM SEARCHAPP.SEARCH_KEYWORD
		INNER JOIN BIOMART.bio_marker bm ON bm.BIO_MARKER_ID = SEARCH_KEYWORD.BIO_DATA_ID
		INNER JOIN deapp.de_snp_gene_map gmap ON gmap.entrez_gene_id = bm.PRIMARY_EXTERNAL_ID
		INNER JOIN DEAPP.DE_RC_SNP_INFO snpinfo ON gmap.snp_name = snpinfo.rs_id
		WHERE KEYWORD=? AND snpinfo.hg_version = ?
	'''

	private static final String geneLimitsSqlQueryById = '''
		SELECT BIO_MARKER_ID, max(snpinfo.pos) as high, min(snpinfo.pos) as low, min(snpinfo.chrom) as chrom
		from BIOMART.bio_marker bm
		INNER JOIN deapp.de_snp_gene_map gmap ON gmap.entrez_gene_id = bm.PRIMARY_EXTERNAL_ID
		INNER JOIN DEAPP.DE_RC_SNP_INFO snpinfo ON gmap.snp_name = snpinfo.rs_id
		WHERE BIO_MARKER_ID = ? AND snpinfo.hg_version = ?
		GROUP BY BIO_MARKER_ID
	'''

	private static final String geneLimitsSqlQueryByEntrez = '''
		SELECT BIO_MARKER_ID, max(snpinfo.pos) as high, min(snpinfo.pos) as low, min(snpinfo.chrom) as chrom
		from deapp.de_snp_gene_map gmap
		INNER JOIN DEAPP.DE_RC_SNP_INFO snpinfo ON gmap.snp_name = snpinfo.rs_id
		INNER JOIN BIOMART.BIO_MARKER bm ON CAST(gmap.entrez_gene_id as varchar(200)) = bm.PRIMARY_EXTERNAL_ID AND bm.PRIMARY_SOURCE_CODE = 'Entrez'
		WHERE gmap.entrez_gene_id = ? AND snpinfo.hg_version = ?
		GROUP BY BIO_MARKER_ID
	'''

	private static final String genePositionSqlQuery = '''
		SELECT DISTINCT BIO_MARKER_ID, ENTREZ_GENE_ID, BIO_MARKER_NAME, BIO_MARKER_DESCRIPTION
		FROM deapp.de_snp_gene_map gmap
		INNER JOIN DEAPP.DE_RC_SNP_INFO snpinfo ON gmap.snp_name = snpinfo.rs_id
		INNER JOIN BIOMART.BIO_MARKER bm ON bm.primary_external_id = CAST(gmap.entrez_gene_id as varchar(200))
		WHERE chrom = ? AND pos >= ? AND pos <= ? AND HG_VERSION = ?
	'''

	// changed study_name to accession because GWAVA needs short name.
	private static final String modelInfoSqlQuery = '''
		SELECT baa.bio_assay_analysis_id as id, ext.model_name as modelName, baa.analysis_name as analysisName, be.accession as studyName
		FROM BIOMART.bio_assay_analysis baa
		LEFT JOIN BIOMART.bio_assay_analysis_ext ext ON baa.bio_assay_analysis_id = ext.bio_assay_analysis_id
		LEFT JOIN BIOMART.bio_experiment be ON baa.etl_id = be.accession
		WHERE baa.bio_assay_data_type = ?
	'''

	//added additional query to pull gene strand information from the annotation.
	private static final String getGeneStrand = '''
		select strand
		from DEAPP.de_gene_info
		where gene_source_id=1
		  and entrez_id=?
	'''

	private static final String getRecombinationRatesForGeneQuery = '''
		select position,rate
		from biomart.bio_recombination_rates recomb,
		(select CASE WHEN chrom_start between 0 and ? THEN 0 ELSE (chrom_start-?) END s, (chrom_stop+?) e, chrom
		 from deapp.de_gene_info g
		 where gene_symbol=?
		 order by chrom_start) geneSub
		where recomb.chromosome=(geneSub.chrom)
		  and position between s and e
		order by position
	'''

	private static final String snpSearchQuery = '''
        with data_subset as
        (
        select gwas.rs_id rs_id, LOG_P_VALUE, analysis_name||' - '||bio_experiment.accession analysis_name from BIOMART.bio_assay_analysis_gwas gwas
        join biomart.bio_assay_analysis analysis on (gwas.bio_assay_analysis_id=analysis.bio_assay_analysis_id)
        left outer join biomart.bio_assay_analysis_ext bax on analysis.bio_assay_analysis_id = bax.bio_assay_analysis_id
        JOIN biomart.bio_experiment on bio_experiment.accession=analysis.etl_id
        where gwas.bio_assay_analysis_id in (_analysisIds_)
        )
        select * from (select snps.rs_id rs_id, chrom, pos, gene_name as gene, exon_intron, recombination_rate, regulome_score from deapp.de_rc_snp_info snps,
        (select pos+? sta, pos-? sto, chrom c from deapp.de_rc_snp_info
        where
        RS_ID =? and
        hg_version=?) ak
        where pos between sto and sta and hg_version=? and chrom=c ) ann_res
        join data_subset on (data_subset.rs_id=ann_res.rs_id)
	'''

	private static final String analysisDataSqlQueryGwas = '''
		SELECT gwas.rs_id as rsid, gwas.bio_asy_analysis_gwas_id as resultid, gwas.bio_assay_analysis_id as analysisid, 
		       gwas.p_value as pvalue, gwas.log_p_value as logpvalue, be.accession as studyname, baa.analysis_name as analysisname, 
		       baa.bio_assay_data_type AS datatype, info.pos as posstart, info.chrom as chromosome, info.gene_name as gene,
		       info.exon_intron as intronexon, info.recombination_rate as recombinationrate, info.regulome_score as regulome
		FROM biomart.Bio_Assay_Analysis_Gwas gwas
		LEFT JOIN deapp.de_rc_snp_info info ON gwas.rs_id = info.rs_id
		LEFT JOIN biomart.Bio_Assay_Analysis baa ON baa.bio_assay_analysis_id = gwas.bio_assay_analysis_id
		LEFT JOIN biomart.bio_experiment be ON be.accession = baa.etl_id
		WHERE (info.pos BETWEEN ? AND ?)
		  AND chrom = ?
		  AND info.hg_version = ?
    	  AND gwas.bio_assay_analysis_id IN (
	'''

	private static final String analysisDataSqlQueryEqtl = '''
		SELECT eqtl.rs_id as rsid, eqtl.bio_asy_analysis_data_id as resultid, eqtl.bio_assay_analysis_id as analysisid,
		       eqtl.p_value as pvalue, eqtl.log_p_value as logpvalue, be.title as studyname, baa.analysis_name as analysisname,
		       baa.bio_assay_data_type AS datatype, info.pos as posstart, info.chrom as chromosome, info.gene_name as gene,
		       info.exon_intron as intronexon, info.recombination_rate as recombinationrate, info.regulome_score as regulome
		FROM biomart.Bio_Assay_Analysis_eqtl eqtl
		LEFT JOIN deapp.de_rc_snp_info info ON eqtl.rs_id = info.rs_id
		LEFT JOIN biomart.Bio_Assay_Analysis baa ON baa.bio_assay_analysis_id = eqtl.bio_assay_analysis_id
		LEFT JOIN biomart.bio_experiment be ON be.accession = baa.etl_id
		WHERE (info.pos BETWEEN ? AND ?)
		  AND chrom = ?
		  AND info.hg_version = ?
		  AND eqtl.bio_assay_analysis_id IN (
	'''

	private static final String recombinationRateBySnpQuery = '''
		WITH snp_info AS (
		    SELECT DISTINCT
		      pos - ? as low,
		      pos + ? as high,
		      chrom
		    FROM DEAPP.DE_RC_SNP_INFO
		    WHERE RS_ID=? and hg_version=?
		)
		SELECT chromosome, position, rate, map
		FROM BIOMART.BIO_RECOMBINATION_RATES
		WHERE POSITION > (SELECT low FROM snp_info)
		  AND POSITION < (SELECT high FROM snp_info)
		  AND CHROMOSOME = (SELECT chrom FROM snp_info) order by position
	'''
}
