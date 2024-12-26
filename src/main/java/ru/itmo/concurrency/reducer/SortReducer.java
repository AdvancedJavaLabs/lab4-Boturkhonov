package ru.itmo.concurrency.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.itmo.concurrency.dto.SortDto;

import java.io.IOException;

public class SortReducer extends Reducer<LongWritable, SortDto, Text, Text> {

    @Override
    protected void reduce(final LongWritable key, final Iterable<SortDto> values, final Context context)
            throws IOException, InterruptedException {
        for (final SortDto value : values) {
            final double revenue = key.get() / 100.0;
            context.write(
                    new Text(value.getCategory()),
                    new Text(revenue + "\t" + value.getQuantity()));
        }
    }
}