package es.ulpgc.matrix.partitioning.coordinates;

import es.ulpgc.matrix.partitioning.Coordinate;

public class ValueCoordinate implements Coordinate {

    public final String matrix;
    public final int x;
    public final int y;
    public final long value;

    public ValueCoordinate(String[] rawCoordinate) {
        this.matrix = rawCoordinate[0];
        this.x = Integer.parseInt(rawCoordinate[1]);
        this.y = Integer.parseInt(rawCoordinate[2]);
        this.value = Long.parseLong(rawCoordinate[3]);
    }
}
