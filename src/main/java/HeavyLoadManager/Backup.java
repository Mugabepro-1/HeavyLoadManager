package HeavyLoadManager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Backup {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/heavyload";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "mupro";

    private static final String CSV_FILE = "output.csv";
    private static final int THREAD_COUNT = 10;
    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        try (BufferedReader reader = new BufferedReader(new FileReader(CSV_FILE))) {
            String header = reader.readLine();
            if (header == null || header.isEmpty()) {
                System.out.println("The CSV file is empty or missing the header row.");
                return;
            }

            long totalRecords = reader.lines().count();
            System.out.println("Total Records: " + totalRecords);

            AtomicInteger totalRecordsInserted = new AtomicInteger(0);
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

            reader.close();
            BufferedReader dataReader = new BufferedReader(new FileReader(CSV_FILE));
            dataReader.readLine();

            String[] rows = dataReader.lines().toArray(String[]::new);
            int recordsPerThread = (int) Math.ceil((double) rows.length / THREAD_COUNT);

            for (int i = 0; i < THREAD_COUNT; i++) {
                int start = i * recordsPerThread;
                int end = Math.min(start + recordsPerThread, rows.length);
                String[] chunk = new String[end - start];
                System.arraycopy(rows, start, chunk, 0, end - start);

                executorService.submit(new DataInserter(chunk, totalRecordsInserted));
            }

            executorService.shutdown();
            while (!executorService.isTerminated()) {
            }

            long endTime = System.currentTimeMillis();
            long elapsedTime = (endTime - startTime) / 1000;
            System.out.println("Data insertion completed. Total Records Inserted: " + totalRecordsInserted.get());
            System.out.println("Time taken: " + elapsedTime / 60 + " minutes and " + elapsedTime % 60 + " seconds.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class DataInserter implements Runnable {
        private final String[] rows;
        private final AtomicInteger totalRecordsInserted;

        DataInserter(String[] rows, AtomicInteger totalRecordsInserted) {
            this.rows = rows;
            this.totalRecordsInserted = totalRecordsInserted;
        }

        @Override
        public void run() {
            String insertSQL = "INSERT INTO load (name, email, address, age) VALUES (?, ?, ?, ?)";

            try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                 PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {

                connection.setAutoCommit(false);

                int batchCount = 0;
                for (String row : rows) {
                    String[] values = parseCsvRow(row);

                    preparedStatement.setString(1, values[0]);
                    preparedStatement.setString(2, values[1]);
                    preparedStatement.setString(3, values[2]);
                    preparedStatement.setInt(4, Integer.parseInt(values[3]));

                    preparedStatement.addBatch();
                    batchCount++;

                    if (batchCount % BATCH_SIZE == 0) {
                        preparedStatement.executeBatch();
                        connection.commit();
                        batchCount = 0;
                    }

                    totalRecordsInserted.incrementAndGet();
                }

                preparedStatement.executeBatch();
                connection.commit();

                System.out.println("Thread " + Thread.currentThread().getName() + " completed processing its chunk.");

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private String[] parseCsvRow(String row) {
            List<String> fields = new ArrayList<>();
            boolean inQuotes = false;
            StringBuilder field = new StringBuilder();

            for (char c : row.toCharArray()) {
                if (c == '"' && (field.length() == 0 || row.charAt(field.length() - 1) != '\\')) {
                    inQuotes = !inQuotes;
                } else if (c == ',' && !inQuotes) {
                    fields.add(field.toString().trim());
                    field = new StringBuilder();
                } else {
                    field.append(c);
                }
            }
            fields.add(field.toString().trim());
            return fields.toArray(new String[0]);
        }
    }
}
