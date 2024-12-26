package ru.itmo.concurrency.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.itmo.concurrency.dto.SalesDto;

import java.io.IOException;

public class SalesReducer extends Reducer<Text, SalesDto, Text, Text> {

    @Override
    protected void reduce(final Text key, final Iterable<SalesDto> values, final Context context)
            throws IOException, InterruptedException {
        long totalRevenue = 0L;
        int totalQuantity = 0;

        for (final SalesDto val : values) {
            totalRevenue += val.getRevenue();
            totalQuantity += val.getQuantity();
        }

        context.write(key, new Text(String.format("%d\t%d", totalRevenue, totalQuantity)));
    }

}
