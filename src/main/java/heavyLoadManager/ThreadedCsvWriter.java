package heavyLoadManager;

import java.io.*;
import java.sql.*;
import java.util.concurrent.*;

public class ThreadedCsvWriter {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/heavyload";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "mupro";
    private static final String OUTPUT_FILE = "people.csv";
    private static final int THREAD_COUNT = 10; // Number of threads for exporting and writing
    private static final int BATCH_SIZE = 100000; // Number of rows per thread

    public static void main(String[] args) {
        long startTime = System.nanoTime(); // Start timing the entire export process

        exportToCSV();

        long endTime = System.nanoTime(); // End timing
        long durationMillis = (endTime - startTime) / 1_000_000;
        System.out.println("Total time taken for export: " + durationMillis + " ms");
    }

    public static void exportToCSV() {
        // Initialize CSV file with header
        try (BufferedWriter csvWriter = new BufferedWriter(new FileWriter(OUTPUT_FILE))) {
            csvWriter.append("Name,Email,Address,Age\n");
        } catch (IOException e) {
            System.out.println("Error initializing CSV file: " + e.getMessage());
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        // Determine the total number of records
        int totalRecords = getTotalRecords();
        if (totalRecords == -1) {
            System.out.println("Failed to fetch total record count.");
            executor.shutdown();
            return;
        }

        System.out.println("Total records: " + totalRecords);

        // Split records into chunks for threads
        int chunkCount = (totalRecords + BATCH_SIZE - 1) / BATCH_SIZE;

        for (int i = 0; i < chunkCount; i++) {
            int start = i * BATCH_SIZE;
            int end = Math.min(start + BATCH_SIZE, totalRecords);

            // Submit both data fetching and writing tasks
            executor.submit(new CSVExportTask(start, end));
        }

        executor.shutdown();
        while (!executor.isTerminated()) {
            // Wait for all threads to finish
        }

        System.out.println("Data successfully exported to " + OUTPUT_FILE);
    }

    private static int getTotalRecords() {
        String countQuery = "SELECT COUNT(*) AS total FROM person";
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement preparedStatement = connection.prepareStatement(countQuery);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            if (resultSet.next()) {
                return resultSet.getInt("total");
            }
        } catch (Exception e) {
            System.out.println("Error fetching total record count: " + e.getMessage());
        }
        return -1;
    }

    static class CSVExportTask implements Runnable {
        private final int start;
        private final int end;

        CSVExportTask(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            long threadStartTime = System.nanoTime(); // Start timing for this thread

            String query = "SELECT name, email, address, age FROM person OFFSET ? LIMIT ?";

            try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                 PreparedStatement preparedStatement = connection.prepareStatement(query)) {

                preparedStatement.setInt(1, start);
                preparedStatement.setInt(2, end - start);

                try (ResultSet resultSet = preparedStatement.executeQuery();
                     BufferedWriter csvWriter = new BufferedWriter(new FileWriter(OUTPUT_FILE, true))) {

                    StringBuilder batch = new StringBuilder();

                    while (resultSet.next()) {
                        String name = resultSet.getString("name");
                        String email = resultSet.getString("email");
                        String address = resultSet.getString("address");
                        int age = resultSet.getInt("age");

                        // Validate the data before adding it to the batch
                        if (name == null || email == null || address == null || age <= 0) {
                            System.err.println("Skipping invalid record: " + name + ", " + email);
                            continue;  // Skip invalid record
                        }

                        // Wrap the address in quotes to handle commas inside
                        address = "\"" + address.replace("\"", "\"\"") + "\"";

                        // Append the row to the batch
                        batch.append(name).append(",")
                                .append(email).append(",")
                                .append(address).append(",")
                                .append(age).append("\n");
                    }

                    // Write the batch to CSV file, synchronized to avoid concurrent writes
                    synchronized (csvWriter) {
                        csvWriter.write(batch.toString());
                    }

                    long threadEndTime = System.nanoTime(); // End timing for this thread
                    long threadDurationMillis = (threadEndTime - threadStartTime) / 1_000_000;
                    System.out.println("Thread exported records " + start + " to " + end + " in " + threadDurationMillis + " ms");

                } catch (IOException e) {
                    System.out.println("Error writing to CSV: " + e.getMessage());
                }

            } catch (Exception e) {
                System.out.println("Error in thread for records " + start + " to " + end + ": " + e.getMessage());
            }
        }
    }
}
