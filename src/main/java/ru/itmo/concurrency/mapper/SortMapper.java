package ru.itmo.concurrency.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.itmo.concurrency.dto.SortDto;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, LongWritable, SortDto> {

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws
            IOException,
            InterruptedException {
        final String[] fields = value.toString().split("\t");

        if (fields.length == 3) {
            try {
                final String categoryKey = fields[0];
                final long revenue = Long.parseLong(fields[1]);
                final int quantity = Integer.parseInt(fields[2]);
                context.write(new LongWritable(revenue), new SortDto(categoryKey, quantity));
            } catch (final NumberFormatException e) {
                System.err.println("Skipping invalid record: " + value);
            }
        }
    }
}
