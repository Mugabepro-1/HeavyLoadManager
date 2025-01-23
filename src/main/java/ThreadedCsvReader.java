import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.sql.*;
import java.util.concurrent.*;

public class ThreadedCsvReader {

    // Method to insert data into the database
    public static void insertData(Connection conn, String name, String email, String address, int age) throws SQLException {
        String sql = "INSERT INTO load (name, email, address, age) VALUES (?, ?, ?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, name);
            stmt.setString(2, email);
            stmt.setString(3, address);
            stmt.setInt(4, age);
            stmt.executeUpdate();
        }
    }

    // Task for inserting data into the database in a multithreaded way
    public static class InsertTask implements Callable<Void> {
        private final String name;
        private final String email;
        private final String address;
        private final int age;

        // Use a new connection for each task to ensure thread safety
        public InsertTask(String name, String email, String address, int age) {
            this.name = name;
            this.email = email;
            this.address = address;
            this.age = age;
        }

        @Override
        public Void call() throws SQLException {
            // Create a new connection for each thread
            try (Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/heavyload", "postgres", "mupro")) {
                insertData(conn, name, email, address, age);
            }
            return null;
        }
    }

    public static void main(String[] args) {
        String csvFile = "people.csv";

        try (FileReader reader = new FileReader(csvFile)) {
            ExecutorService executor = Executors.newFixedThreadPool(10); // Using 10 threads
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().withQuote('"').parse(reader); // Ensure quoted fields are handled correctly

            for (CSVRecord record : records) {
                String name = record.get("Name");
                String email = record.get("Email");
                String address = record.get("Address");

                // Trim the "Age" field to remove any leading/trailing spaces
                String ageStr = record.get("Age").trim();
                int age = 0;

                // Check if the age value is numeric before parsing
                try {
                    age = Integer.parseInt(ageStr);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid age value: " + ageStr + " for " + name);
                    continue; // Skip this record if age is invalid
                }

                // Submit the task to the executor for parallel execution
                executor.submit(new InsertTask(name, email, address, age));
            }

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS); // Wait for all tasks to complete

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
