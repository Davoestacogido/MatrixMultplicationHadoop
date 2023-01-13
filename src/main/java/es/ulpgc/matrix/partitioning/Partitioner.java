package es.ulpgc.matrix.partitioning;



import org.apache.hadoop.fs.Path;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class Partitioner {
    public Path partitionate(File pathToMatrixes, int numberOfSubmatrixes)  {
        String allSubmatrixes = "";
        List<List<String>> rawMatrixes = readMatrixes(pathToMatrixes);
        int size = rawMatrixes.get(0).get(0).split(" ").length;
        allSubmatrixes = getSubmatrixes("A", rawMatrixes.get(0), size, allSubmatrixes, createCurrentSubmatrixes(numberOfSubmatrixes), numberOfSubmatrixes);
        allSubmatrixes = getSubmatrixes("B", rawMatrixes.get(1), size, allSubmatrixes, createCurrentSubmatrixes(numberOfSubmatrixes), numberOfSubmatrixes);
        System.out.println(allSubmatrixes);

        return createSubMatrixesFile(allSubmatrixes);
    }

    private Path createSubMatrixesFile(String allSubmatrixes) {
        createDirectory(java.nio.file.Path.of("./datamart/"));
        String file =  "./datamart/" +  UUID.randomUUID();
        try(FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            fileOutputStream.write(allSubmatrixes.getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Path(file);
    }

    private void createDirectory(java.nio.file.Path path) {
        if (!Files.exists(path)) path.toFile().mkdirs();
    }

    private String getSubmatrixes(String name,List<String> matrixARows, int size, String allSubmatrixes, List<List<String>> currentSubmatrixes, int numberOfSubmatrixes) {
        for (String line : matrixARows) {
            for (int i = 0; i < numberOfSubmatrixes; i++)
                for (int j = 0; j < size/numberOfSubmatrixes; j++)
                    currentSubmatrixes.get(i).add(line.split(" ")[j + i * numberOfSubmatrixes]);
            if (areSubmatrixesFull(size/numberOfSubmatrixes, currentSubmatrixes))
                allSubmatrixes = createNewSubmatrixes(currentSubmatrixes, allSubmatrixes, numberOfSubmatrixes, name);
        }
        return allSubmatrixes;
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

    private List<List<String>> createCurrentSubmatrixes(int numberOfSubmatrixes) {
        List<List<String>> currentSubmatrixes = new ArrayList<>(numberOfSubmatrixes);
        for (int i = 0; i < numberOfSubmatrixes; i++)
            currentSubmatrixes.add(new ArrayList<>());
        return currentSubmatrixes;
    }

    private String createNewSubmatrixes(List<List<String>> currentSubmatrixes, String allSubmatrixes, int numberOfSubmatrixes, String name) {
        StringBuilder allSubmatrixesBuilder = new StringBuilder(allSubmatrixes);
        for (int i = 0; i < numberOfSubmatrixes; i++) {
            allSubmatrixesBuilder.append(name).append(" ").append(String.join(" ", currentSubmatrixes.get(i))).append("\n");
            currentSubmatrixes.set(i, new ArrayList<>());
        }
        return allSubmatrixesBuilder.toString();
    }
}
