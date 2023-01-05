package es.ulpgc.matrix.partitioning;

import es.ulpgc.matrix.partitioning.coordinates.ValueCoordinate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text item, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        ValueCoordinate coordinate = new ValueCoordinate(item.toString().split(","));
        if (coordinate.matrix.equals("A")) addAvalueToMultiply(context, Integer.parseInt(conf.get("size")), coordinate);
        else addBvalueToMultiply(context, Integer.parseInt(conf.get("size")), coordinate);
    }

    private void addBvalueToMultiply(Context context, int size, ValueCoordinate coordinate) throws IOException, InterruptedException {
        for (int row = 0; row < size; row++) {
            context.write(
                    new Text(row + "," + coordinate.y),
                    new Text("B," + coordinate.x + "," + coordinate.value)
            );
        }
    }

    private void addAvalueToMultiply(Context context, int size, ValueCoordinate coordinate) throws IOException, InterruptedException {
        for (int col = 0; col < size; col++) {
            context.write(
                    new Text(coordinate.x + "," + col),
                    new Text(coordinate.matrix + "," + coordinate.y + "," + coordinate.value)
            );
        }
    }
}
