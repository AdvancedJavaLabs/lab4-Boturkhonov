package ru.itmo.concurrency.dto;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SalesDto implements Writable {

    private long revenue;

    private int quantity;

    public SalesDto() {
    }

    public SalesDto(long revenue, int quantity) {
        this.revenue = revenue;
        this.quantity = quantity;
    }

    public long getRevenue() {
        return revenue;
    }

    public int getQuantity() {
        return quantity;
    }

    public void set(long revenue, int quantity) {
        this.revenue = revenue;
        this.quantity = quantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(revenue);
        out.writeInt(quantity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        revenue = in.readLong();
        quantity = in.readInt();
    }

    @Override
    public String toString() {
        return revenue + "\t" + quantity;
    }
}