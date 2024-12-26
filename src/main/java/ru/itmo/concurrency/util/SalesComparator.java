package ru.itmo.concurrency.util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SalesComparator extends WritableComparator {

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return b.compareTo(a);
    }

}
