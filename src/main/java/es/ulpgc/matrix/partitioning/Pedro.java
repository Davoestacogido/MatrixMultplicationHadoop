package es.ulpgc.matrix.partitioning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;


public class Pedro {


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("size", "1000");
        Job job = Job.getInstance(conf, "pedro");
        job.setJarByClass(Pedro.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); //"src/main/resources"
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //result
        job.waitForCompletion(true);
    }

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>{


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

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

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
}
