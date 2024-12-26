package ru.itmo.concurrency.dto;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortDto implements Writable {

    private String category;

    private int quantity;

    public SortDto() {}

    public SortDto(String category, int quantity) {
        this.category = category;
        this.quantity = quantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(category);
        out.writeInt(quantity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        category = in.readUTF();
        quantity = in.readInt();
    }

    public String getCategory() {
        return category;
    }

    public int getQuantity() {
        return quantity;
    }
}