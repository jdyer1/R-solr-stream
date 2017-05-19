package rsolrstream;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import rsolrstream.testthat.MockSolrClient;

public class TestMockSolrClient {
    @Test
    public void test() throws Exception {
        MockSolrClient msc = new MockSolrClient();
        RStreamingExpressionIterator iter = RStreamingExpressions.executeStreamingExpression(msc,
            "excecuting_collection_name", "expression(data_collection_name, la-de-la-la)");
        Set<String> validColumnNames = new HashSet<>(Arrays.asList(new String[] { "string", "double", "long", "boolean",
            "string_multi", "double_multi", "long_multi", "boolean_multi" }));

        for (String colName : iter.columnNames()) {
            Assert.assertTrue(colName + " is not in the valid-set", validColumnNames.contains(colName));
        }
        int docNum = 1;
        while (iter.hasNext()) {
            RStreamingExpressionRow row = iter.next();
            
            int j = 0;
            for(int i : iter.stringIndexes()) {
                String[] arr = row.getStrings(j);
                String colName = iter.columnNames()[i];
                Assert.assertTrue(colName + " is wrong for the data.", colName.startsWith("string"));
                if(arr.length==1) {
                    Assert.assertTrue(colName + " is wrong for the data.", !colName.endsWith("_multi"));
                    Assert.assertEquals(msc.getString(docNum), arr[0]);
                } else {
                    Assert.assertTrue(colName + " is wrong for the data.", colName.endsWith("_multi"));
                    Assert.assertArrayEquals(msc.getStringMulti(docNum), arr);
                }
                j++;
            }
            
            j = 0;
            for(int i : iter.booleanIndexes()) {
                boolean[] arr = row.getBooleans(j);
                String colName = iter.columnNames()[i];
                Assert.assertTrue(colName + " is wrong for the data.", colName.startsWith("boolean"));
                if(arr.length==1) {
                    Assert.assertTrue(colName + " is wrong for the data.", !colName.endsWith("_multi"));
                    Assert.assertEquals(msc.getBoolean(docNum), arr[0]);
                } else {
                    Assert.assertTrue(colName + " is wrong for the data.", colName.endsWith("_multi"));
                    boolean[] barr = msc.getBooleanMulti(docNum);
                    for(int k=0 ; k<barr.length ; k++) {
                        Assert.assertEquals(barr[k], arr[k]);
                    }
                }
                j++;
            }
            
            j = 0;
            for(int i : iter.longIndexes()) {
                long[] arr = row.getLongs(j);
                String colName = iter.columnNames()[i];
                Assert.assertTrue(colName + " is wrong for the data.", colName.startsWith("long"));
                if(arr.length==1) {
                    Assert.assertTrue(colName + " is wrong for the data.", !colName.endsWith("_multi"));
                    Assert.assertEquals(msc.getLong(docNum), arr[0]);
                } else {
                    Assert.assertTrue(colName + " is wrong for the data.", colName.endsWith("_multi"));
                    Assert.assertArrayEquals(msc.getLongMulti(docNum), arr);
                }
                j++;
            }
            
            j = 0;
            for(int i : iter.doubleIndexes()) {
                double[] arr = row.getDoubles(j);
                String colName = iter.columnNames()[i];
                Assert.assertTrue(colName + " is wrong for the data.", colName.startsWith("double"));
                if(arr.length==1) {
                    Assert.assertTrue(colName + " is wrong for the data.", !colName.endsWith("_multi"));
                    Assert.assertEquals(msc.getDouble(docNum), arr[0], .01);
                } else {
                    Assert.assertTrue(colName + " is wrong for the data.", colName.endsWith("_multi"));
                    Assert.assertArrayEquals(msc.getDoubleMulti(docNum), arr, .01);
                }
                j++;
            }
            
            docNum++;

        }
    }
}
