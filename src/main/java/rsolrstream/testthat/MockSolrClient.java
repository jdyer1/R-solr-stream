package rsolrstream.testthat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.util.NamedList;
import org.noggit.JSONUtil;

public class MockSolrClient extends SolrClient {
    private static final long serialVersionUID = 1L;
    private static final Map<String, Object> d1;
    private static final Map<String, Object> d2;
    private static final Map<String, Object> root;

    private boolean closed = false;

    static {
        root = new LinkedHashMap<>();

        d1 = new LinkedHashMap<>();
        d1.put("string", "PORK");
        d1.put("string_multi", new String[] { "hope", "life", "peace" });
        d1.put("double", Math.PI);
        d1.put("boolean_multi", new boolean[] { true, true });
        d1.put("long", -12345678900l);
        d1.put("double_multi", new double[] { 1.2, 3.4, -1.2, -3.4 });
        d1.put("boolean", true);
        d1.put("long_multi", new long[] { 1, 2, 3, 4, 5, 6 });

        d2 = new LinkedHashMap<>();
        d2.put("string", "print_corn <- function { print(\"CORN\"); }");
        d2.put("string_multi", new String[] { "death", "destruction", "strife" });
        d2.put("double", Double.NaN);
        d2.put("boolean_multi", new boolean[] { true, false });
        d2.put("long", 9876543210l);
        d2.put("double_multi", new double[] { Math.E, Double.MIN_VALUE });
        d2.put("boolean", true);
        d2.put("long_multi", new long[] { 10000000000l, 20000000000l, 30000000000l });

        Map<String, Object> eof = new LinkedHashMap<>();
        eof.put("EOF", true);
        eof.put("RESPONSE_TIME", 123);

        Map<String, Object> docs = new LinkedHashMap<>();
        List<Map<String, Object>> docList = new ArrayList<>();
        root.put("result-set", docs);
        docs.put("docs", docList);
        docList.add(d1);
        docList.add(d2);
        docList.add(eof);
    }

    public String getString(int docNum) {
        return (String) doc(docNum).get("string");
    }

    public String[] getStringMulti(int docNum) {
        return (String[]) doc(docNum).get("string_multi");
    }

    public boolean getBoolean(int docNum) {
        return (boolean) doc(docNum).get("boolean");
    }

    public boolean[] getBooleanMulti(int docNum) {
        return (boolean[]) doc(docNum).get("boolean_multi");
    }

    public long getLong(int docNum) {
        return (long) doc(docNum).get("long");
    }

    public long[] getLongMulti(int docNum) {
        return (long[]) doc(docNum).get("long_multi");
    }

    public double getDouble(int docNum) {
        return (double) doc(docNum).get("double");
    }

    public double[] getDoubleMulti(int docNum) {
        return (double[]) doc(docNum).get("double_multi");
    }

    private Map<String, Object> doc(int docNum) {
        return docNum == 1 ? d1 : docNum == 2 ? d2 : Collections.emptyMap();
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    @Override
    public NamedList<Object> request(@SuppressWarnings("rawtypes") SolrRequest req, String collection)
        throws SolrServerException, IOException {
        if (closed) {
            throw new SolrServerException("The SolrClient is closed.");
        }
        if (collection == null) {
            throw new SolrServerException("a collection must be specified");
        }
        String expr = req.getParams().get("expr");
        if (expr == null) {
            throw new SolrServerException(
                "there should be an 'expr' parameter on the request, to specify a streaming expression.");
        }
        String json = JSONUtil.toJSON(root);
        InputStream is = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        NamedList<Object> nl = new NamedList<>(Collections.singletonMap("stream", is));
        return nl;
    }
}
