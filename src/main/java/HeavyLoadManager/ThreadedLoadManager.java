package HeavyLoadManager;

import com.github.javafaker.Faker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadedLoadManager {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/heavyload";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "mupro";
    private static final int TOTAL_RECORDS = 10000000;
    private static final int THREAD_COUNT = 10; // Number of threads
    private static final int BATCH_SIZE = 100000; // Batch size per thread

    public static void main(String[] args) {
        // Start timing
        long startTime = System.nanoTime();

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        int recordsPerThread = TOTAL_RECORDS / THREAD_COUNT;

        for (int i = 0; i < THREAD_COUNT; i++) {
            int start = i * recordsPerThread + 1;
            int end = (i == THREAD_COUNT - 1) ? TOTAL_RECORDS : (start + recordsPerThread - 1);

            executor.execute(new DatabaseInsertTask(start, end, i + 1));
        }

        executor.shutdown();
        while (!executor.isTerminated()) {
            // Wait for all threads to finish
        }

        // End timing
        long endTime = System.nanoTime();

        // Calculate total time taken
        long durationInMillis = (endTime - startTime) / 1_000_000;
        System.out.println("Total time taken to insert " + TOTAL_RECORDS + " records: " + durationInMillis + " ms");
    }

    static class DatabaseInsertTask implements Runnable {
        private final int start;
        private final int end;
        private final int threadId;

        DatabaseInsertTask(int start, int end, int threadId) {
            this.start = start;
            this.end = end;
            this.threadId = threadId;
        }

        @Override
        public void run() {
            long threadStartTime = System.nanoTime(); // Start timing for this thread
            Faker faker = new Faker();
            String insertSQL = "INSERT INTO person(name, email, address, age) VALUES (?, ?, ?, ?)";

            try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                 PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {

                connection.setAutoCommit(false);

                int count = 0;
                for (int i = start; i <= end; i++) {
                    String name = faker.name().fullName();
                    String email = faker.internet().emailAddress();
                    String address = faker.address().fullAddress();
                    int age = faker.number().numberBetween(18, 100);

                    preparedStatement.setString(1, name);
                    preparedStatement.setString(2, email);
                    preparedStatement.setString(3, address);
                    preparedStatement.setInt(4, age);

                    preparedStatement.addBatch();

                    if (++count % BATCH_SIZE == 0) {
                        preparedStatement.executeBatch();
                        connection.commit();
                        System.out.println("Thread " + threadId + " inserted " + count + " records so far.");
                    }
                }

                // Insert remaining records
                preparedStatement.executeBatch();
                connection.commit();

                long threadEndTime = System.nanoTime(); // End timing for this thread
                long threadDurationInMillis = (threadEndTime - threadStartTime) / 1_000_000;
                System.out.println("Thread " + threadId + " finished inserting records from " + start + " to " + end + ". Total inserted: " + count + ". Time taken: " + threadDurationInMillis + " ms");

            } catch (SQLException e) {
                System.out.println("Thread " + threadId + " encountered an error: " + e.getMessage());
            }
        }
    }
}
