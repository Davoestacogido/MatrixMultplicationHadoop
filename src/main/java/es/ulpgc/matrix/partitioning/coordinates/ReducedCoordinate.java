package es.ulpgc.matrix.partitioning.coordinates;

import es.ulpgc.matrix.partitioning.Coordinate;

public class ReducedCoordinate implements Coordinate {

    public final String matrix;
    public final int position;
    public final long value;

    public ReducedCoordinate(String[] rawReduced) {
        this.matrix = rawReduced[0];
        this.position = Integer.parseInt(rawReduced[1]);
        this.value = Long.parseLong(rawReduced[2]);
    }
}
