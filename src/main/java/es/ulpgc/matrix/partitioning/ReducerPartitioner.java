package es.ulpgc.matrix.partitioning;

import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ReducerPartitioner extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text position, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(null, new Text(position + "," + sumValues(values)));
    }

    private long sumValues(Iterable<Text> values) {
        long sum = 0;
        for (Text val : values)
            sum = sum + Long.parseLong(val.toString());
        return sum;
    }

}
