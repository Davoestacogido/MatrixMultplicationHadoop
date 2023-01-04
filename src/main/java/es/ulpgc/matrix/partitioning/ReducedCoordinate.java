package es.ulpgc.matrix.partitioning;

public class ReducedCoordinate {

    public final String matrix;
    public final int position;
    public final long value;

    public ReducedCoordinate(String[] rawReduced) {
        this.matrix = rawReduced[0];
        this.position = Integer.parseInt(rawReduced[1]);
        this.value = Long.parseLong(rawReduced[2]);
    }
}
