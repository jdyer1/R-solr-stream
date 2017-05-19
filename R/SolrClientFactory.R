solr_client_from_zookeeper_hosts <- function(zookeeper_hosts_comma_separated, zookeeper_chroot=.jnull(class = "java/lang/String")) {
	sc <- .jcall("rsolrstream/SolrClientFactory", "Lorg/apache/solr/client/solrj/SolrClient;", "fromZookeeperHosts", zookeeper_hosts_comma_separated, zookeeper_chroot)
	sc
}

solr_client_from_solr_hosts <- function(solr_hosts_comma_separated) {
	sc <- .jcall("rsolrstream/SolrClientFactory", "Lorg/apache/solr/client/solrj/SolrClient;", "fromSolrHosts", solr_hosts_comma_separated)
	sc
}