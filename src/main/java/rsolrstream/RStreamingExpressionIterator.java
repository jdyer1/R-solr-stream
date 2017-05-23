package rsolrstream;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.io.stream.JSONTupleStream;
import org.apache.solr.client.solrj.io.stream.TupleStreamParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RStreamingExpressionIterator implements Iterator<RStreamingExpressionRow>, Closeable {
    private static final Logger log = LoggerFactory.getLogger(RStreamingExpressions.class);
    private final TupleStreamParser tsp;
    private boolean closed = false;
    private Map<String, Object> next = null;
    private String[] columnNames;
    private TYPES[] columnTypes;
    private int[] booleanIndexes;
    private int[] longIndexes;
    private int[] doubleIndexes;
    private int[] stringIndexes;

    private enum TYPES {
                        BOOLEAN, STRING, DOUBLE, LONG
    };

    RStreamingExpressionIterator(Reader r, String[] columnNames) {
        tsp = new JSONTupleStream(r);
        this.columnNames = columnNames;
        populateColumnTypesAndIndexes();
        log.debug("Start ts: {}", System.currentTimeMillis());
    }

    RStreamingExpressionIterator(Reader r) {
        tsp = new JSONTupleStream(r);
        populateColumnNames();
        populateColumnTypesAndIndexes();
        log.debug("Start ts: {}", System.currentTimeMillis());
    }

    public String[] columnNames() {
        log.debug("column names: {}", Arrays.toString(columnNames));
        return columnNames;
    }

    public int[] booleanIndexes() {
        log.debug("boolean indexes: {}", Arrays.toString(booleanIndexes));
        return booleanIndexes;
    }

    public int[] longIndexes() {
        log.debug("long indexes: {}", Arrays.toString(longIndexes));
        return longIndexes;
    }

    public int[] stringIndexes() {
        log.debug("string indexes: {}", Arrays.toString(stringIndexes));
        return stringIndexes;
    }

    public int[] doubleIndexes() {
        log.debug("double indexes: {}", Arrays.toString(doubleIndexes));
        return doubleIndexes;
    }

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
        internalNext();
        return next != null;
    }

    @Override
    public RStreamingExpressionRow next() {
        if (next == null) {
            internalNext();
        }
        if (next != null) {
            Map<String, Object> returnVal = next;
            next = null;
            return convertRow(returnVal);
        }
        return null;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                tsp.close();
            } catch (Exception e) {
                log.warn("Could not close TupleStreamParser.", e);
            }
        }
    }

    private void internalNext() {
        if (!closed) {
            try {
                next = tsp.next();
            } catch (IOException e) {
                close();
                throw new RuntimeException(e);
            }
            if (next == null) {
                next = null;
                close();
            } else if (next.containsKey("EOF")) {
                next = null;
                log.debug("End timestamp: {}", System.currentTimeMillis());
                close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private RStreamingExpressionRow convertRow(Map<String, Object> m) {
        boolean[][] b = new boolean[booleanIndexes.length][];
        long[][] l = new long[longIndexes.length][];
        double[][] d = new double[doubleIndexes.length][];
        String[][] s = new String[stringIndexes.length][];
        int bp = 0;
        int lp = 0;
        int dp = 0;
        int sp = 0;

        for (int i = 0; i < columnNames.length; i++) {
            Object o = m.get(columnNames[i]);
            if (o == null) {
                switch (columnTypes[i]) {
                    case BOOLEAN:
                        b[bp++] = new boolean[] { false };
                        break;
                    case LONG:
                        l[lp++] = new long[] { 0 };
                        break;
                    case DOUBLE:
                        d[dp++] = new double[] { Double.NaN };
                        break;
                    case STRING:
                        s[sp++] = new String[] { "" };
                        break;
                }
            } else if (o instanceof List) {
                List<?> list = ((List<?>) o);
                switch (columnTypes[i]) {
                    case BOOLEAN:
                        b[bp++] = unboxBooleanList((List<Boolean>) list);
                        break;
                    case LONG:
                        l[lp++] = unboxLongList((List<Long>) list);
                        break;
                    case DOUBLE:
                        d[dp++] = unboxDoubleList((List<Double>) list);
                        break;
                    case STRING:
                        s[sp++] = list.toArray(new String[0]);
                        break;
                }
            } else {
                switch (columnTypes[i]) {
                    case BOOLEAN:
                        b[bp++] = new boolean[] { (Boolean) o };
                        break;
                    case LONG:
                        l[lp++] = new long[] { (Long) o };
                        break;
                    case DOUBLE:
                        try {
                            d[dp] = new double[] { (Double) o };
                        } catch (ClassCastException e) {
                            d[dp] = new double[] { Double.parseDouble(o.toString()) };
                        }
                        dp++;
                        break;
                    case STRING:
                        s[sp++] = new String[] { (String) o };
                        break;
                }
            }
        }
        return new RStreamingExpressionRow(s, b, d, l);
    }

    private void populateColumnTypesAndIndexes() {
        if (!hasNext()) {
            columnTypes = new TYPES[0];
            booleanIndexes = new int[0];
            longIndexes = new int[0];
            doubleIndexes = new int[0];
            stringIndexes = new int[0];
        } else {
            columnTypes = new TYPES[columnNames.length];
            List<Integer> bil = new ArrayList<>(columnNames.length);
            List<Integer> lil = new ArrayList<>(columnNames.length);
            List<Integer> dil = new ArrayList<>(columnNames.length);
            List<Integer> sil = new ArrayList<>(columnNames.length);
            int i = 0;
            for (String columnName : columnNames) {
                Object val = next.get(columnName);
                if (val != null) {
                    if (val instanceof List) {
                        val = ((List<?>) val).get(0);
                    }
                    if (val instanceof Boolean) {
                        columnTypes[i] = TYPES.BOOLEAN;
                        bil.add(i);
                    } else if (val instanceof Double) {
                        columnTypes[i] = TYPES.DOUBLE;
                        dil.add(i);
                    } else if (val instanceof Long) {
                        columnTypes[i] = TYPES.LONG;
                        lil.add(i);
                    } else {
                        columnTypes[i] = TYPES.STRING;
                        sil.add(i);
                    }
                } else {
                    columnTypes[i] = TYPES.STRING;
                    sil.add(i);
                }
                i++;
            }
            booleanIndexes = unboxIntegerList(bil);
            longIndexes = unboxIntegerList(lil);
            doubleIndexes = unboxIntegerList(dil);
            stringIndexes = unboxIntegerList(sil);
        }
    }

    private void populateColumnNames() {
        if (!hasNext()) {
            columnNames = new String[0];
        } else {
            columnNames = new String[next.size()];
            int i = 0;
            for (Map.Entry<String, Object> entry : next.entrySet()) {
                columnNames[i] = entry.getKey();
                i++;
            }
        }
    }

    private int[] unboxIntegerList(List<Integer> l) {
        int[] ia = new int[l.size()];
        for (int i = 0; i < l.size(); i++) {
            ia[i] = l.get(i);
        }
        return ia;
    }

    private boolean[] unboxBooleanList(List<Boolean> l) {
        boolean[] ba = new boolean[l.size()];
        for (int i = 0; i < l.size(); i++) {
            ba[i] = l.get(i);
        }
        return ba;
    }

    private long[] unboxLongList(List<Long> l) {
        long[] la = new long[l.size()];
        for (int i = 0; i < l.size(); i++) {
            la[i] = l.get(i);
        }
        return la;
    }

    private double[] unboxDoubleList(List<?> l) {
        double[] da = new double[l.size()];
        for (int i = 0; i < l.size(); i++) {
            try {
                da[i] = (Double) l.get(i);
            } catch (ClassCastException e) {
                // This handles the string representations for NaN, Infinity, etc.
                da[i] = Double.parseDouble(l.get(i).toString());
            }
        }
        return da;
    }
}