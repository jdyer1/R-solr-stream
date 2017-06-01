package rsolrstream;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.UpdateStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackgroundStreamingExpression implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(BackgroundStreamingExpression.class);

    private static final ThreadPoolExecutor tpe = new ThreadPoolExecutor(1, Runtime.getRuntime().availableProcessors(),
        60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10));

    private static final StreamFactory sf = new StreamFactory().withFunctionName(RStream.FUNCTION_NAME, RStream.class)
        .withFunctionName("update", UpdateStream.class);

    private final TupleStream stream;
    private final StreamContext context;

    public BackgroundStreamingExpression(CloudSolrClient csc, String expression) throws IOException {
        log.debug("Creating BackgroundStreamingExpression for: {}", expression);
        try {
            stream = sf.constructStream(expression);
            context = new StreamContext();
            SolrClientCache scCache = new RSolrClientCache(csc);
            context.setSolrClientCache(scCache);
            stream.setStreamContext(context);
        } catch (IOException e) {
            log.error("Could not create BackgroundStreamingExpression", e);
            throw e;
        }
    }

    public void submit() {
        tpe.execute(this);
    }

    @Override
    public void run() {
        try {
            stream.open();
            int i = 0;
            for (Tuple t = stream.read(); !t.EOF; t = stream.read()) {
                int j = 0;
                for (Object o : t.fields.entrySet()) {
                    Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
                    log.debug(i + " | " + j + " | " + entry.getKey() + " | " + entry.getValue());
                    j++;
                }
                i++;
            }
        } catch (IOException e) {
            log.error("Problem streaming: ", e);
        } finally {
            try {
                stream.close();
            } catch (IOException e1) {
                log.warn("Problem closing stream: ", e1);
            }
        }
    }
}
