package rsolrstream;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RStreamingExpressions {
    private static final Logger log = LoggerFactory.getLogger(RStreamingExpressions.class);

    public static RStreamingExpressionIterator executeStreamingExpression(SolrClient sc, String collection,
        String expression) throws IOException, SolrServerException {
        if (!checkResponseWriter(sc)) {
            log.error("The SolrClient's Response Writer should be: " + InputStreamResponseParser.class.getName());
        }
        SolrParams sp = new MapSolrParams(Collections.singletonMap("expr", expression));
        SolrRequest<?> sr = new GenericSolrRequest(METHOD.POST, "/stream", sp);
        NamedList<Object> nl = sc.request(sr, collection);
        InputStream stream = (InputStream) nl.get("stream");
        InputStreamReader reader = new InputStreamReader(stream, "UTF-8");
        return new RStreamingExpressionIterator(reader);
    }
    
    private static boolean checkResponseWriter(SolrClient sc) {
        if (sc instanceof HttpSolrClient && !(((HttpSolrClient) sc).getParser() instanceof InputStreamResponseParser)) {
            return false;
        } else if (sc instanceof LBHttpSolrClient
            && !(((LBHttpSolrClient) sc).getParser() instanceof InputStreamResponseParser)) {
            return false;
        } else if (sc instanceof CloudSolrClient
            && !(((CloudSolrClient) sc).getParser() instanceof InputStreamResponseParser)) {
            return false;
        }
        return true;
    }
}
