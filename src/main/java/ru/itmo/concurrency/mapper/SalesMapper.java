package ru.itmo.concurrency.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.itmo.concurrency.dto.SalesDto;

import java.io.IOException;

public class SalesMapper extends Mapper<Object, Text, Text, SalesDto> {
    private final Text categoryKey = new Text();

    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        final String[] fields = value.toString().split(",");

        if (fields.length == 5 && !"transaction_id".equals(fields[0])) {
            try {
                final String category = fields[2];
                final long price = (long) (Double.parseDouble(fields[3]) * 100);
                final int quantity = Integer.parseInt(fields[4]);
                categoryKey.set(category);
                context.write(categoryKey, new SalesDto(price * quantity, quantity));
            } catch (final NumberFormatException e) {
                System.err.println("Skipping invalid record: " + value);
            }
        }
    }
}
