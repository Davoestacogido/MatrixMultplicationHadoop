package es.ulpgc.matrix.partitioning;

import es.ulpgc.matrix.partitioning.coordinates.SubMatrix;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MapperPartitioner extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text item, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        SubMatrix A = new SubMatrix(item.toString().split("-")[0].split(" "));
        SubMatrix B = new SubMatrix(item.toString().split("-")[1].split(" "));
        multiply(A,B, context);
    }

    private void multiply(SubMatrix a, SubMatrix b, Context context) throws IOException, InterruptedException {
        for (int i = 0; i < a.size; i++)
            for (int j = 0; j < a.size; j++)
                for (int k = 0; k < a.size; k++) {
                    context.write(
                            new Text(i + a.x*a.size + " " + j + b.x*b.size),
                            new Text(String.valueOf(a.values[i][k] * b.values[k][j]))
                    );
            }
        }
}
