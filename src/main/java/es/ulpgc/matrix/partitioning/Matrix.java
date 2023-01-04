package es.ulpgc.matrix.partitioning;

import java.util.Arrays;

public interface Matrix {
    long value(int row, int col);
    long[][] raw();
    int size();
    double density();
    default double sparsity() {
        return 1 - density();
    }

    static Matrix create(long[][] values) {
        return new Matrix() {
            @Override
            public long value(int row, int col) {
                return values[row][col];
            }

            @Override
            public long[][] raw() {
                return values;
            }

            @Override
            public int size() {
                return values.length;
            }

            @Override
            public double density() {
                double nonZeroValues = Math.toIntExact(Arrays.stream(values)
                        .flatMapToLong(Arrays::stream)
                        .filter(value -> value != 0d)
                        .count());
                return nonZeroValues / (Math.pow(values.length, 2));
            }
        };
    }
}