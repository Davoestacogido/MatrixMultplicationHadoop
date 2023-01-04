package es.ulpgc.matrix.partitioning;

public class Coordinate {

    public final String matrix;
    public final int x;
    public final int y;
    public final long value;

    public Coordinate(String[] rawCoordinate) {
        this.matrix = rawCoordinate[0];
        this.x = Integer.parseInt(rawCoordinate[1]);
        this.y = Integer.parseInt(rawCoordinate[2]);
        this.value = Long.parseLong(rawCoordinate[3]);
    }

    @Override
    public String toString() {
        return "Coordinate{" +
                "matrix='" + matrix + '\'' +
                ", x=" + x +
                ", y=" + y +
                ", value=" + value +
                '}';
    }
}
