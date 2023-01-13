package es.ulpgc.matrix.partitioning;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;


public class Driver {

    public static void main(String[] args) {

        Path subMatrixes = new Partitioner().partitionate(new File("C:\\Users\\Usuario\\IdeaProjects\\MatrixMultplicationHadoop\\src\\main\\resources"), 2);
//        Configuration conf = new Configuration();
//        conf.set("size", 3 + "");
//        Job job = Job.getInstance(conf, "Matrix Multiplication");
//        job.setJarByClass(Driver.class);
//        job.setMapperClass(Mapper.class);
//        job.setReducerClass(Reducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path("C:\\Users\\Usuario\\IdeaProjects\\MatrixMultplicationHadoop\\src\\main\\resources"));
//        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\Usuario\\IdeaProjects\\MatrixMultplicationHadoop\\src\\main\\result"));
//        job.waitForCompletion(true);
    }
}
