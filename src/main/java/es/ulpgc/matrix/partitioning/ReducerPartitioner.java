package es.ulpgc.matrix.partitioning;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;

public class ReducerPartitioner extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text position, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long sum = sumValues(values);
        context.write(null, new Text(position + "," + sum));
        System.out.println(position);
    }

    private long sumValues(Iterable<Text> values) {
        Iterator<Text> iterator = values.iterator();
        long sum = 0;
        for (Text val : values){
            sum = sum + Long.parseLong(iterator.next().toString());
            System.out.println(val);
        }
        System.out.println("-----------------------------");
        return sum;
    }

}
