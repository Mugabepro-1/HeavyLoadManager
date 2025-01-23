package heavyLoadManager;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class CsvToDbMultithread {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/heavyload";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "mupro";
    private static final int THREAD_COUNT = 10; // Adjust based on your CPU cores
    private static final int BATCH_SIZE = 100000;
    private static long totalRecordsInserted = 0;  // Track the total inserted records

    public static void main(String[] args) throws IOException, InterruptedException {
        String csvFilePath = "people.csv";

        long startTime = System.nanoTime();  // Start time for total execution

        // Read CSV in chunks and process
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        try (CSVReader csvReader = new CSVReader(new FileReader(csvFilePath))) {
            String[] header = csvReader.readNext(); // Skip header
            List<String[]> batch = new ArrayList<>(BATCH_SIZE);
            String[] line;

            while ((line = csvReader.readNext()) != null) {
                batch.add(line);
                if (batch.size() == BATCH_SIZE) {
                    executor.submit(new DataInserter(new ArrayList<>(batch)));
                    batch.clear();
                }
            }

            // Submit remaining records
            if (!batch.isEmpty()) {
                executor.submit(new DataInserter(batch));
            }
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        long endTime = System.nanoTime();  // End time for total execution
        long duration = (endTime - startTime) / 1_000_000_000;  // Convert to seconds
        System.out.println("Data insertion completed in " + duration + " seconds!");
    }

    static class DataInserter implements Runnable {
        private final List<String[]> records;

        public DataInserter(List<String[]> records) {
            this.records = records;
        }

        @Override
        public void run() {
            String sql = "INSERT INTO load (name, email, address, age) VALUES (?, ?, ?, ?)";
            try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                 PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

                connection.setAutoCommit(false);

                for (String[] record : records) {
                    try {
                        preparedStatement.setString(1, record[0]);
                        preparedStatement.setString(2, record[1]);
                        preparedStatement.setString(3, record[2]);
                        preparedStatement.setInt(4, Integer.parseInt(record[3]));
                        preparedStatement.addBatch();
                    } catch (Exception e) {
                        System.err.println("Skipping invalid record: " + Arrays.toString(record));
                        continue;  // Skip invalid record
                    }
                }

                preparedStatement.executeBatch();
                connection.commit();

                // Log the progress after each batch insertion
                synchronized (CsvToDbMultithread.class) {
                    totalRecordsInserted += records.size();
                    if (totalRecordsInserted % 100000 == 0) {
                        System.out.println("Inserted " + totalRecordsInserted + " records so far...");
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
