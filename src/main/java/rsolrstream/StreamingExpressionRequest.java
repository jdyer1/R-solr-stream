package rsolrstream;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;

public class StreamingExpressionRequest extends SolrRequest<SimpleSolrResponse> {
    private static final long serialVersionUID = 1L;
    private final SolrParams params;
    private final SimpleSolrResponse response = new SimpleSolrResponse();

    public StreamingExpressionRequest(String expression) {
        super(METHOD.POST, "/stream");
        this.params = new MapSolrParams(Collections.singletonMap("expr", expression));
    }

    @Override
    public SolrParams getParams() {
        return params;
    }

    @Override
    public Collection<ContentStream> getContentStreams() throws IOException {
        return null;
    }

    @Override
    protected SimpleSolrResponse createResponse(SolrClient client) {
        return response;
    }
    
    

}
