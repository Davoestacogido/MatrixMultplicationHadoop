package es.ulpgc.matrix.partitioning;

import es.ulpgc.matrix.partitioning.coordinates.ReducedCoordinate;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class ReducerPartitioner extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text position, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = sumValues(values);
        context.write(position, new Text(String.valueOf(sum)));
    }

    private double sumValues(Iterable<Text> values) {
        Iterator<Text> iterator = values.iterator();
        long sum = 0;
        while (iterator.hasNext()) sum = Long.parseLong(sum + iterator.next().toString());
        return sum;
    }

}
