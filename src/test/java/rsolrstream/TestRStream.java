package rsolrstream;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestRStream {
    @Test
    public void test() throws Exception {
        RInputRow[] rows = new RInputRow[2];
        rows[0] = new RInputRow(4);
        rows[0].setBoolean(0, true);
        rows[0].setDouble(1, Math.PI);
        rows[0].setLong(2, 12345678900l);
        rows[0].setString(3, "This is a String");

        rows[1] = new RInputRow(4);
        rows[1].setBoolean(0, false);
        rows[1].setDouble(1, Math.E);
        rows[1].setLong(2, -987654321);
        rows[1].setString(3, "This is another String");

        RInputRow eof = new RInputRow(0);

        BlockingQueue<RInputRow> queue = new ArrayBlockingQueue<>(rows.length + 1);
        for (int i = 0; i < rows.length; i++) {
            queue.add(rows[i]);
        }
        queue.add(eof);

        String queueName = this.getClass().getName() + "-" + System.currentTimeMillis();
        RStream.registerQueue(queueName, queue);
        StreamFactory sf = new StreamFactory().withFunctionName(RStream.FUNCTION_NAME, RStream.class);
        String expr =
            RStream.FUNCTION_NAME + "(sort=\"foo asc\", readTimeoutMillis=1000, queueName=\"" + queueName + "\")";
        TupleStream ts = sf.constructStream(expr);
        ts.setStreamContext(new StreamContext());
        try {
            ts.open();
            int i = 0;
            for (Tuple t = ts.read(); !t.EOF; t = ts.read()) {
                int j = 0;
                for (Object o : t.fields.entrySet()) {
                    Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
                    Assert.assertEquals("Column Names is not as expected", "X" + j, entry.getKey());
                    Assert.assertEquals("Data is not the same", rows[i].getDataArray()[j], entry.getValue());
                    j++;
                }
                i++;
            }
        } finally {
            ts.close();
        }
    }
}
