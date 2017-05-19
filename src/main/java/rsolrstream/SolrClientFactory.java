package rsolrstream;

import java.util.Arrays;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;

public class SolrClientFactory {

    public static SolrClient fromZookeeperHosts(String zkHostsCommaSeparated, String zkChroot) {
        CloudSolrClient.Builder b =
            new CloudSolrClient.Builder().withZkHost(Arrays.asList(zkHostsCommaSeparated.split(",")));
        if (zkChroot != null && zkChroot.length() > 0) {
            b.withZkChroot(zkChroot);
        }
        CloudSolrClient csc = b.build();
        csc.setParser(new InputStreamResponseParser("json"));
        return csc;
    }

    public static SolrClient fromSolrHosts(String solrHostsCommaSeparated) {
        String[] hostArr = solrHostsCommaSeparated.split(",");
        if (hostArr.length == 1) {
            return new HttpSolrClient.Builder(hostArr[0]).withResponseParser(new InputStreamResponseParser("json"))
                .build();
        }
        return new LBHttpSolrClient.Builder().withBaseSolrUrls(hostArr)
            .withResponseParser(new InputStreamResponseParser("json")).build();
    }
}
