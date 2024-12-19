package ru.itmo.concurrency.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SalesReducer extends Reducer<Text, IntWritable, Text, Text> {
    private final Text result = new Text();

    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
            throws IOException, InterruptedException {
        int totalRevenue = 0;
        int totalQuantity = 0;

        for (final IntWritable val : values) {
            totalRevenue += val.get();
            totalQuantity++;
        }

        result.set(totalRevenue / 100.0 + "\t" + totalQuantity); // Возвращаем в формате: выручка + количество
        context.write(key, result);
    }
}
