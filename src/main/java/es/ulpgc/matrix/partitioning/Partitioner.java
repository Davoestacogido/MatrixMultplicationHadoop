package es.ulpgc.matrix.partitioning;


import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

public class Partitioner {

    private int size;
    private final List<String> allSubMatrixes = new ArrayList<>();

    public void partitionate(File pathToMatrixes, int numberOfSubMatrixes)  {
        List<List<String>> rawMatrixes = readMatrixes(pathToMatrixes);
        size = rawMatrixes.get(0).get(0).split(" ").length;
        if (size % numberOfSubMatrixes != 0 ) throw new RuntimeException(numberOfSubMatrixes + " is not an avaible number of submatrixes for a " + size + "x" + size + " matrixes.");
        getSubMatrixes("A", rawMatrixes.get(0), createCurrentSubMatrixes(numberOfSubMatrixes), numberOfSubMatrixes);
        getSubMatrixes("B", rawMatrixes.get(1), createCurrentSubMatrixes(numberOfSubMatrixes), numberOfSubMatrixes);
        String allMapperItems = prepareToMultiply(numberOfSubMatrixes);
        createSubMatrixesFile(allMapperItems);
    }

    private String prepareToMultiply(int numberOfSubMatrixes) {
        StringBuilder allMapperItems = new StringBuilder();
        for (String subMatrixA : allSubMatrixes.subList(0, numberOfSubMatrixes*numberOfSubMatrixes))
            for (String subMatrixB : allSubMatrixes.subList(numberOfSubMatrixes*numberOfSubMatrixes, allSubMatrixes.size())) {
                if (shouldMultiply(subMatrixA, subMatrixB))
                    allMapperItems.append(subMatrixA).append(";").append(subMatrixB).append("\n");
            }
        return allMapperItems.toString();
    }

    private static boolean shouldMultiply(String subMatrixA, String subMatrixB) {
        return Objects.equals(subMatrixA.split(" ")[1], subMatrixB.split(" ")[0]);
    }

    private void createSubMatrixesFile(String allSubmatrixes) {
        createDirectory(java.nio.file.Path.of("./datamart/"));
        String file =  "./datamart/" +  UUID.randomUUID();
        try(FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            fileOutputStream.write(allSubmatrixes.getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void createDirectory(java.nio.file.Path path) {
        if (!Files.exists(path)) path.toFile().mkdirs();
    }

    private void getSubMatrixes(String name, List<String> matrixARows, List<List<String>> currentSubMatrixes, int numberOfSubMatrixes) {
        int currentCol = 0;
        for (String matrixARow : matrixARows) {
            for (int i = 0; i < numberOfSubMatrixes; i++)
                for (int j = 0; j < size / numberOfSubMatrixes; j++)
                    currentSubMatrixes.get(i).add(matrixARow.split(" ")[j + i * (size / numberOfSubMatrixes)]);
            if (areSubmatrixesFull(size / numberOfSubMatrixes, currentSubMatrixes)) {
                createNewSubmatrixes(currentSubMatrixes, allSubMatrixes, numberOfSubMatrixes, name, currentCol);
                currentCol++;
            }
        }
    }

    private static List<List<String>> readMatrixes(File path) {
        return Arrays.stream(path.listFiles())
                .map(file -> {
                    try {
                        return new BufferedReader(new InputStreamReader(new FileInputStream(file))).lines().collect(Collectors.toList());
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
    }

    private static boolean areSubmatrixesFull(int submatrixesSize, List<List<String>> currentSubmatrixes) {
        return currentSubmatrixes.get(0).size() == submatrixesSize*submatrixesSize;
    }

    private List<List<String>> createCurrentSubMatrixes(int numberOfSubmatrixes) {
        List<List<String>> currentSubmatrixes = new ArrayList<>(numberOfSubmatrixes);
        for (int i = 0; i < numberOfSubmatrixes; i++)
            currentSubmatrixes.add(new ArrayList<>());
        return currentSubmatrixes;
    }

    private void createNewSubmatrixes(List<List<String>> currentSubmatrixes, List<String> allSubmatrixes, int numberOfSubmatrixes, String name, int currentCol) {
        for (int i = 0; i < numberOfSubmatrixes; i++) {
            allSubmatrixes.add(currentCol + " " + i + " " + name + " " + String.join(" ", currentSubmatrixes.get(i)));
            currentSubmatrixes.set(i, new ArrayList<>());
        }
    }
}
