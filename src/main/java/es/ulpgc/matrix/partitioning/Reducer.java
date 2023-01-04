package es.ulpgc.matrix.partitioning;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        HashMap<Integer, Long> hashA = new HashMap<>();
        HashMap<Integer, Long> hashB = new HashMap<>();
        for (Text val : values) {
            ReducedCoordinate coordinate = new ReducedCoordinate(val.toString().split(","));
            if (coordinate.matrix.equals("A"))
                hashA.put(coordinate.position, coordinate.value);
            else
                hashB.put(coordinate.position, coordinate.value);
        }
        int size = Integer.parseInt(context.getConfiguration().get("size"));
        long result = 0;
        long m_ij;
        long n_jk;
        for (int j = 0; j < size; j++) {
            m_ij = hashA.getOrDefault(j, 0L);
            n_jk = hashB.getOrDefault(j, 0L);
            result += m_ij * n_jk;
        }
        if (result != 0L)
            context.write(null, new Text(key + "," + result));
    }
}