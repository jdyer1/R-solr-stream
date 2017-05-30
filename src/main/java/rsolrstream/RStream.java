package rsolrstream;

import static org.apache.solr.common.params.CommonParams.SORT;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;

public class RStream extends TupleStream implements Expressible {
    private static final long serialVersionUID = 1L;

    public static final String FUNCTION_NAME = "R";
    private static ConcurrentHashMap<String, BlockingQueue<RInputRow>> queues = new ConcurrentHashMap<>();
    private final StreamExpression expression;
    private final StreamComparator userDefinedSort;
    private final BlockingQueue<RInputRow> queue;
    private final String[] columnNames;
    private final int readTimeoutMillis;
    

    public RStream(StreamExpression expression, StreamFactory factory) throws IOException {
        this.expression = expression;
        
        try {
            userDefinedSort = factory.constructComparator(
                ((StreamExpressionValue) factory.getNamedOperand(expression, SORT).getParameter()).getValue(),
                FieldComparator.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Missing or invalid parameter value: " + SORT);
        }
        
        try {
            StreamExpressionNamedParameter senp = factory.getNamedOperand(expression, "queueName");
            StreamExpressionValue sev = (StreamExpressionValue) senp.getParameter();
            String queueName = sev.getValue();
            queue = queues.get(queueName);
        } catch (Exception e) {
            throw new IllegalArgumentException("Missing or invalid 'queueName'");
        }
        if(queue==null) {
            throw new IllegalStateException("The specified queue must first be registered before attempting to use it.");
        }
        
        this.readTimeoutMillis = factory.getIntOperand(expression, "readTimeoutMillis", 60000);
        
        String[] _columnNames = new String[0];
        try {
            StreamExpressionNamedParameter senp = factory.getNamedOperand(expression, "columnNames");
            StreamExpressionValue sev = (StreamExpressionValue) senp.getParameter();
            String columnNamesCommaSeparated = sev.getValue();
            _columnNames = columnNamesCommaSeparated.split(",\\s*");
        } catch (Exception e) {
           //ignore
        }
        this.columnNames = _columnNames;
    }
    
    public static void registerQueue(String queueName, BlockingQueue<RInputRow> queue) {
        if(queues.putIfAbsent(queueName, queue) != null) {
            throw new IllegalArgumentException("The queue " + queueName +  " already exists");
        }
    }

    @Override
    public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
        return expression;
    }

    @Override
    public void setStreamContext(StreamContext context) {
        // no-op

    }

    @Override
    public List<TupleStream> children() {
        return Collections.emptyList();
    }

    @Override
    public void open() throws IOException {
        // no-op
    }

    @Override
    public void close() throws IOException {
        // no-op
    }

    @Override
    public Tuple read() throws IOException {
        RInputRow row;
        try {
            row = queue.poll(readTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch(InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread was interrupted.");
        }
        if(row==null) {
            throw new IOException("Read Timeout. Max wait is " + readTimeoutMillis + " ms.");
        }
        Object[] dataArray = row.getDataArray();
        if(dataArray.length==0) {
            return new Tuple(Collections.singletonMap("EOF", Boolean.TRUE));
        }
        Map<String,Object> dataMap = new HashMap<>();        
        for(int i=0 ; i<dataArray.length ; i++ ) {
            Object o = dataArray[i];
            String colName = columnNames.length > i ? columnNames[i] : "X" + i;
            dataMap.put(colName, o);
        }
        return new Tuple(dataMap);
    }

    @Override
    public StreamComparator getStreamSort() {
        return userDefinedSort;
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {
        return new StreamExplanation(getStreamNodeId().toString())
            .withFunctionName(FUNCTION_NAME).withImplementingClass(this.getClass().getName())
            .withExpressionType(ExpressionType.STREAM_DECORATOR).withExpression(toExpression(factory).toString());
    }

}
