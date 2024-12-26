package ru.itmo.concurrency.util;

import org.apache.hadoop.conf.Configuration;

public class HadoopUtil {

    private static final int BLOCK_SIZE = 128 * 1024 * 1024;

    public
    static Configuration createHadoopConfiguration() {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.fileinputformat.split.maxsize", Integer.toString(BLOCK_SIZE));
        conf.set("mapreduce.input.fileinputformat.split.minsize", Integer.toString(BLOCK_SIZE));

        return conf;
    }

}
