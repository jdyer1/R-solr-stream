package rsolrstream;

import java.io.IOException;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RSolrClientCache extends SolrClientCache {
    private static final long serialVersionUID = 1L;

    private static final Logger log = LoggerFactory.getLogger(RSolrClientCache.class);
    
    private final CloudSolrClient cloudSolrClient;
    private final String zkHost;
    
    public RSolrClientCache(CloudSolrClient cloudSolrClient) {
        this.zkHost = cloudSolrClient.getZkHost();
        this.cloudSolrClient = cloudSolrClient;
    }
    
    @Override
    public synchronized CloudSolrClient getCloudSolrClient(String zkHost) {
        if(zkHost.equals(this.zkHost)) {
            return this.cloudSolrClient;
        }
        return super.getCloudSolrClient(zkHost);
    }
    
    @Override
    public synchronized void close() {
        try {
            cloudSolrClient.close();
        } catch(IOException ioe) {
            log.error("Problem closing SolrClient for " + zkHost, ioe);
        }
        super.close();
    }
}
