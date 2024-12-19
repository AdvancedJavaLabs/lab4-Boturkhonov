package ru.itmo.concurrency;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import ru.itmo.concurrency.mapper.SalesMapper;
import ru.itmo.concurrency.reducer.SalesReducer;

public class Main {
    public static void main(String[] args) throws Exception {

        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "Sales Analysis");

        job.setJarByClass(Main.class);
        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Указываем входную директорию с файлами CSV
        FileInputFormat.addInputPath(job, new Path(Main.class.getResource("/csv_files").toURI()));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}