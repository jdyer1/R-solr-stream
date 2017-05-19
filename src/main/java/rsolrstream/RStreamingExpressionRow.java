package rsolrstream;

public class RStreamingExpressionRow {
    public final String[][] strings;
    public final boolean[][] booleans;
    public final double[][] doubles;
    public final long[][] longs;

    public RStreamingExpressionRow(String[][] strings, boolean[][] booleans, double[][] doubles, long[][] longs) {
        this.strings = strings;
        this.booleans = booleans;
        this.doubles = doubles;
        this.longs = longs;
    }

    public String[] getStrings(int index) {
        return strings[index];
    }

    public boolean[] getBooleans(int index) {
        return booleans[index];
    }

    public double[] getDoubles(int index) {
        return doubles[index];
    }

    public long[] getLongs(int index) {
        return longs[index];
    }
}
