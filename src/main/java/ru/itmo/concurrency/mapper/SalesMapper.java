package ru.itmo.concurrency.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalesMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text category = new Text();
    private final IntWritable revenue = new IntWritable();

    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        final String[] fields = value.toString().split(",");
        if (fields.length < 5 || "transaction_id".equals(fields[0])) {
            return; // Пропустить заголовок и некорректные строки
        }

        final String productCategory = fields[2];
        int price = (int) (Double.parseDouble(fields[3]) * 100); // Переводим в целое число (копейки)
        int quantity = Integer.parseInt(fields[4]);

        category.set(productCategory);
        revenue.set(price * quantity);

        context.write(category, revenue);
    }
}
