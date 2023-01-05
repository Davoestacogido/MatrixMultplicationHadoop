package es.ulpgc.matrix.partitioning;

import es.ulpgc.matrix.partitioning.coordinates.ReducedCoordinate;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text position, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<Integer, Long> AcolVals = new HashMap<>();
        Map<Integer, Long> BrowVals = new HashMap<>();
        for (Text val : values)
            prepareValueToMultiply(AcolVals, BrowVals, val);
        writeValue(position, context, multiply(context, AcolVals, BrowVals));
    }

    private void writeValue(Text position, Context context, long result) throws IOException, InterruptedException {
        if (result != 0L) context.write(null, new Text(position + "," + result));
    }

    private long multiply(Context context, Map<Integer, Long> AcolVals, Map<Integer, Long> BrowVals) {
        long result = 0;
        for (int j = 0; j < Integer.parseInt(context.getConfiguration().get("size")); j++)
            result += AcolVals.getOrDefault(j, 0L) * BrowVals.getOrDefault(j, 0L);
        return result;
    }

    private void prepareValueToMultiply(Map<Integer, Long> AcolVals, Map<Integer, Long> BrowVals, Text val) {
        ReducedCoordinate coordinate = new ReducedCoordinate(val.toString().split(","));
        if (coordinate.matrix.equals("A")) AcolVals.put(coordinate.position, coordinate.value);
        else BrowVals.put(coordinate.position, coordinate.value);
    }
}
