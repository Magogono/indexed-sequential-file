//          49. Rekordy pliku: 3 punkty (6 współrzędnych) w układzie kartezjańskim.
//          Uporządkowanie wg pola trójkąta tworzonego przez te współrzędne.

import jdk.nashorn.internal.objects.annotations.Getter;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import static java.lang.Math.abs;


public class Record {

    private class Point {
        private double x;
        private double y;

        private Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        private String getString(){
            return x + ", " + y;
        }
    }

    private Point a;
    private Point b;
    private Point c;

    private int key;
    private int pointerToPage;
    private int pointerToRecord;
    private int isDeleted;
    public static final int RECORD_SIZE = Integer.BYTES + 6*Double.BYTES + 3*Integer.BYTES;

    public Record(int key) {
        a = new Point(0, 0);
        b = new Point(0, 0);
        c = new Point(0, 0);
        this.key = key;
        pointerToPage = -1;
        pointerToRecord = -1;
        isDeleted = 0;
    }

    public Record() {
        this(-1);
    }

    public void setA(double x, double y) {
        this.a.x = x;
        this.a.y = y;
    }

    public void setB(double x, double y) {
        this.b.x = x;
        this.b.y = y;
    }

    public void setC(double x, double y) {
        this.c.x = x;
        this.c.y = y;
    }

    public boolean isDeleted() {
        if(isDeleted == 1)
            return true;
        else
            return false;
    }

    public void setDeletion(boolean value) {
        if(value == true)
            isDeleted = 1;
        else
            isDeleted = 0;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public void setKey() {
        MathContext MATH_CTX = new MathContext(5, RoundingMode.HALF_UP);
        key = new BigDecimal(this.getArea(), MATH_CTX).intValue();
    }

    public void setPointerToNextRecord(int indOfPage, int indOfRecord) {
        pointerToPage = indOfPage;
        pointerToRecord = indOfRecord;
    }

    public int getPointerToPage() {
        return pointerToPage;
    }

    public int getPointerToRecord() {
        return pointerToRecord;
    }

    public int getKey() {
        // setKey();
        return key;
    }

    public double getArea() {
        return 0.5 * abs(
                (b.x - a.x)*(c.y - a.y) - (b.y - a.y)*(c.x - a.x)
        );
    }

    public void copyFrom(Record record) {
        this.setA(record.a.x, record.a.y);
        this.setB(record.b.x, record.b.y);
        this.setC(record.c.x, record.c.y);
        key = record.getKey();
        pointerToPage = record.pointerToPage;
        pointerToRecord = record.pointerToRecord;
        isDeleted = record.isDeleted;
    }

    public  byte[] convertToBytes() {
        ByteBuffer bb = ByteBuffer.allocate(RECORD_SIZE);

        bb.putInt(getKey());
        bb.putDouble(a.x);
        bb.putDouble(a.y);
        bb.putDouble(b.x);
        bb.putDouble(b.y);
        bb.putDouble(c.x);
        bb.putDouble(c.y);
        bb.putInt(pointerToPage);
        bb.putInt(pointerToRecord);
        bb.putInt(isDeleted);

        return bb.array();
    }

    @Override
    public String toString() {
        return "key: " + key + " a: " + a.getString() + " b: " + b.getString() + " c: " + c.getString();
    }
}
