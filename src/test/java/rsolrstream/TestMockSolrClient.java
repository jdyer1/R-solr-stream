package rsolrstream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import rsolrstream.testthat.MockSolrClient;

public class TestMockSolrClient {

    @Test
    public void testAll() throws Exception {
        runTest(null, null);
    }

    @Test
    public void testPartialColumns() throws Exception {
        runTest(new String[] { "string", "double_multi" }, null);
    }

    @Test
    public void testNonexistentColumns() throws Exception {
        runTest(new String[] { "string", "zzz", "double_multi" }, new String[] { "zzz" });
    }

    private void runTest(String[] columns, String[] invalidColumns) throws Exception {
        MockSolrClient msc = new MockSolrClient();
        String mockCollection = "executing_collection_name";
        String mockExpression = "expression(data_collection_name, la-de-la-la)";
        RStreamingExpressionIterator iter =
            RStreamingExpressions.executeStreamingExpression(msc, mockCollection, mockExpression, columns);
        Set<String> validColumnNames = new HashSet<>(Arrays.asList(new String[] { "string", "double", "long", "boolean",
            "string_multi", "double_multi", "long_multi", "boolean_multi" }));

        Set<String> invalidColumnNames = invalidColumns == null ? Collections.emptySet()
            : new HashSet<>(new ArrayList<>(Arrays.asList(invalidColumns)));
        for (String colName : iter.columnNames()) {
            if (!invalidColumnNames.contains(colName)) {
                Assert.assertTrue(colName + " is not in the valid-set", validColumnNames.contains(colName));
            }
        }

        int docNum = 1;
        while (iter.hasNext()) {
            RStreamingExpressionRow row = iter.next();

            int j = 0;
            for (int i : iter.stringIndexes()) {
                String[] arr = row.getStrings(j);
                String colName = iter.columnNames()[i];
                if (!invalidColumnNames.contains(colName)) {
                    Assert.assertTrue(colName + " is wrong for the data.", colName.startsWith("string"));
                    if (arr.length == 1) {
                        Assert.assertTrue(colName + " is wrong for the data.", !colName.endsWith("_multi"));
                        Assert.assertEquals(msc.getString(docNum), arr[0]);
                    } else {
                        Assert.assertTrue(colName + " is wrong for the data.", colName.endsWith("_multi"));
                        Assert.assertArrayEquals(msc.getStringMulti(docNum), arr);
                    }
                    j++;
                }
            }

            j = 0;
            for (int i : iter.booleanIndexes()) {
                boolean[] arr = row.getBooleans(j);
                String colName = iter.columnNames()[i];
                if (!invalidColumnNames.contains(colName)) {
                    Assert.assertTrue(colName + " is wrong for the data.", colName.startsWith("boolean"));
                    if (arr.length == 1) {
                        Assert.assertTrue(colName + " is wrong for the data.", !colName.endsWith("_multi"));
                        Assert.assertEquals(msc.getBoolean(docNum), arr[0]);
                    } else {
                        Assert.assertTrue(colName + " is wrong for the data.", colName.endsWith("_multi"));
                        boolean[] barr = msc.getBooleanMulti(docNum);
                        for (int k = 0; k < barr.length; k++) {
                            Assert.assertEquals(barr[k], arr[k]);
                        }
                    }
                }
                j++;
            }

            j = 0;
            for (int i : iter.longIndexes()) {
                long[] arr = row.getLongs(j);
                String colName = iter.columnNames()[i];
                if (!invalidColumnNames.contains(colName)) {
                    Assert.assertTrue(colName + " is wrong for the data.", colName.startsWith("long"));
                    if (arr.length == 1) {
                        Assert.assertTrue(colName + " is wrong for the data.", !colName.endsWith("_multi"));
                        Assert.assertEquals(msc.getLong(docNum), arr[0]);
                    } else {
                        Assert.assertTrue(colName + " is wrong for the data.", colName.endsWith("_multi"));
                        Assert.assertArrayEquals(msc.getLongMulti(docNum), arr);
                    }
                }
                j++;
            }

            j = 0;
            for (int i : iter.doubleIndexes()) {
                double[] arr = row.getDoubles(j);
                String colName = iter.columnNames()[i];
                if (!invalidColumnNames.contains(colName)) {
                    Assert.assertTrue(colName + " is wrong for the data.", colName.startsWith("double"));
                    if (arr.length == 1) {
                        Assert.assertTrue(colName + " is wrong for the data.", !colName.endsWith("_multi"));
                        Assert.assertEquals(msc.getDouble(docNum), arr[0], .01);
                    } else {
                        Assert.assertTrue(colName + " is wrong for the data.", colName.endsWith("_multi"));
                        Assert.assertArrayEquals(msc.getDoubleMulti(docNum), arr, .01);
                    }
                }
                j++;
            }

            docNum++;

        }
    }
}
