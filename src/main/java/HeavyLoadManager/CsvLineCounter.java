package HeavyLoadManager;

import java.io.*;

public class CsvLineCounter {
    public static void main(String[] args) throws IOException {
        String csvFilePath = "output.csv";
        long lineCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            while (br.readLine() != null) {
                lineCount++;
            }
        }

        System.out.println("Total lines in CSV: " + lineCount);
    }
}
