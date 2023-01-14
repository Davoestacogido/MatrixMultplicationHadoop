package es.ulpgc.matrix.partitioning;

public class SubMatrix {

    public final long[][] values;
    public final String matrix;
    public final int x;
    public final int y;
    public final int size;

    public SubMatrix(String[] rawSubMatrix) {
        size = (int) (Math.sqrt(rawSubMatrix.length - 3));
        x = Integer.parseInt(rawSubMatrix[0]);
        y = Integer.parseInt(rawSubMatrix[1]);
        values = new long[size][size];
        matrix = rawSubMatrix[2];
        for (int i = 0; i < size; i++)
            for (int j = 0; j < size; j++)
                values[j][i] = Long.parseLong(rawSubMatrix[3 + i + j*size]);
    }
}
