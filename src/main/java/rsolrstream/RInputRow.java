package rsolrstream;

import java.util.Arrays;

public class RInputRow {
    private final Object[] row;

    public RInputRow(int size) {
        this.row = new Object[size];
    }

    public void setBoolean(int i, boolean b) {
        row[i] = b;
    }

    public void setLong(int i, long l) {
        row[i] = l;
    }

    public void setDouble(int i, double d) {
        row[i] = d;
    }

    public void setString(int i, String s) {
        row[i] = s;
    }
    
    public Object[] getDataArray() {
        return row;
    }

    public String toString() {
        return Arrays.toString(row);
    }
}
