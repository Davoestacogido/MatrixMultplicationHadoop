package es.ulpgc.matrix.partitioning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;


public class Driver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {


        Path subMatrixes = new Partitioner().partitionate(new File("C:\\Users\\Usuario\\IdeaProjects\\MatrixMultplicationHadoop\\src\\main\\resources"), 2);

        Configuration conf = new Configuration();
        conf.set("size", 3 + "");
        Job job = Job.getInstance(conf, "Matrix Multiplication");
        job.setJarByClass(Driver.class);
        job.setMapperClass(MapperPartitioner.class);
        job.setReducerClass(ReducerPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\Users\\Usuario\\IdeaProjects\\MatrixMultplicationHadoop\\datamart"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\Usuario\\IdeaProjects\\MatrixMultplicationHadoop\\src\\main\\result"));
        job.waitForCompletion(true);
    }
}
