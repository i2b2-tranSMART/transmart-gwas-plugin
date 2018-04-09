grails.hibernate.cache.queries = false
grails.logging.jul.usebridge = true

log4j = {
	error 'org.codehaus.groovy.grails',
	      'org.springframework',
	      'org.hibernate',
	      'net.sf.ehcache.hibernate'

	debug 'org.hibernate.SQL'
	trace 'org.hibernate.type.descriptor.sql.BasicBinder'
}
