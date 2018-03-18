package hw2_4;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Point {
//    String[] strings;

    private double[] corrds;

//    Point(String[] strings) {}

    protected Point() {};

    Point(double[] corrds){
        this.corrds = corrds;
    }

    public double[] getCorrds() {
        return corrds;
    }

    public void setCorrds(double[] corrds) {
        this.corrds = corrds;
    }

    public double euclidian(Point target) {
        double sum = 0;
        double[] targetCoords = target.getCorrds();
        for (int i=0; i<corrds.length; i++) {
            sum += (corrds[i] - targetCoords[i])*(corrds[i] - targetCoords[i]);
        }
        return sum;
    }
}
