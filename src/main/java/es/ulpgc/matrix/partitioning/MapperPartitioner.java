package es.ulpgc.matrix.partitioning;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MapperPartitioner extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text item, Context context) throws IOException, InterruptedException {
        multiply(
                new SubMatrix(item.toString().split(";")[0].split(" ")),
                new SubMatrix(item.toString().split(";")[1].split(" ")),
                context
        );
    }

    private void multiply(SubMatrix a, SubMatrix b, Context context) throws IOException, InterruptedException {
        for (int i = 0; i < a.size; i++)
            for (int j = 0; j < a.size; j++)
                for (int k = 0; k < a.size; k++)
                    context.write(
                            new Text( (i + a.x*a.size) + "," + (j + b.y*b.size)),
                            new Text(String.valueOf(a.values[i][k] * b.values[k][j]))
                    );
    }
}
