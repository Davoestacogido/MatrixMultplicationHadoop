package es.ulpgc.matrix.partitioning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>{


    private org.apache.hadoop.mapreduce.Mapper.Context context;


    @Override
    protected void map(LongWritable key, Text item, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int size = Integer.parseInt(conf.get("size"));
        Coordinate coordinate = new Coordinate(item.toString().split(","));
        Text outputKey = new Text();
        Text outputValue = new Text();
        if (coordinate.matrix.equals("A")) {
            for (int i = 0; i < size; i++) {
                outputKey.set(coordinate.x + "," + i);

                outputValue.set(coordinate.matrix + "," + coordinate.y
                        + "," + coordinate.value);

                context.write(outputKey, outputValue);
            }
        } else {
            for (int i = 0; i < size; i++) {
                outputKey.set(i + "," + coordinate.y);
                outputValue.set("B," + coordinate.x + ","
                        + coordinate.value);
                context.write(outputKey, outputValue);
            }
        }
    }
}