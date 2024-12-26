package ru.itmo.concurrency;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import ru.itmo.concurrency.dto.SalesDto;
import ru.itmo.concurrency.dto.SortDto;
import ru.itmo.concurrency.mapper.SalesMapper;
import ru.itmo.concurrency.mapper.SortMapper;
import ru.itmo.concurrency.reducer.SalesReducer;
import ru.itmo.concurrency.reducer.SortReducer;
import ru.itmo.concurrency.util.SalesComparator;

import java.io.IOException;

import static ru.itmo.concurrency.util.HadoopUtil.createHadoopConfiguration;

public class Main {

    private static final String JOB_NAME = "Sales-Analysis";

    private static final String INPUT_PATH = "input";

    private static final String OUTPUT_PATH = "output";

    private static final String OUTPUT_SORTED_PATH = "output-sorted";

    public static void main(String[] args) throws Exception {
        final Configuration conf = createHadoopConfiguration();

        runSalesJob(conf);
        runSortJob(conf);
    }

    private static void runSalesJob(final Configuration conf)
            throws Exception {

        long startTime = System.currentTimeMillis();
        try (final Job job = createSalesJob(conf)) {
            boolean success = job.waitForCompletion(true);

            if (success) {
                long elapsedTimeInSeconds = (System.currentTimeMillis() - startTime) / 1000;
                final String result = "Time: " + elapsedTimeInSeconds + " seconds\n";
                System.out.println(result);
            } else {
                System.err.println("Job failed.");
            }
        }
    }

    private static void runSortJob(final Configuration conf) throws Exception {
        System.out.println("Running Sort Job...");

        final Job sortJob = Job.getInstance(conf, JOB_NAME + " - Sorting");
        sortJob.setJarByClass(Main.class);
        sortJob.setMapperClass(SortMapper.class);
        sortJob.setReducerClass(SortReducer.class);

        sortJob.setMapOutputKeyClass(LongWritable.class);
        sortJob.setMapOutputValueClass(SortDto.class);
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(Text.class);
        sortJob.setSortComparatorClass(SalesComparator.class);

        FileInputFormat.addInputPath(sortJob, new Path(Main.OUTPUT_PATH));
        FileOutputFormat.setOutputPath(sortJob, new Path(Main.OUTPUT_SORTED_PATH));

        boolean sortSuccess = sortJob.waitForCompletion(true);

        if (!sortSuccess) {
            System.err.println("Sort job failed.");
        } else {
            System.out.println("Sort job completed successfully.");
        }
    }

    private static Job createSalesJob(final Configuration conf)
            throws IOException {
        final Job job = Job.getInstance(conf, JOB_NAME);
        job.setJarByClass(Main.class);
        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SalesDto.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job, new Path(Main.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Main.OUTPUT_PATH));

        return job;
    }
}