package HeavyLoadManager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class ToCsv {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/heavyload";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "mupro";
    private static final String CSV_FILE = "output.csv";
    private static final int THREAD_COUNT = 10;
    private static final int BATCH_SIZE = 100000;

    public static void main(String[] args) {
        try {
            new ToCsv().exportDataToCsv();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void exportDataToCsv() throws SQLException, IOException, InterruptedException {
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            long totalRecords = getRowCount(connection);
            System.out.println("Total Records: " + totalRecords);

            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
            AtomicLong processedCount = new AtomicLong(0);

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(CSV_FILE))) {
                // Write header without 'id'
                writer.write("name,email,address,age");
                writer.newLine();

                for (int i = 0; i < totalRecords; i += BATCH_SIZE) {
                    final int offset = i;
                    executor.submit(() -> {
                        try {
                            writeBatchToCsv(connection, writer, offset, BATCH_SIZE);
                            long count = processedCount.addAndGet(BATCH_SIZE);
                            System.out.println("Processed: " + Math.min(count, totalRecords) + "/" + totalRecords);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                }
                executor.shutdown();
                while (!executor.isTerminated()) {
                    Thread.sleep(1000);
                }
            }
            System.out.println("Data successfully exported to " + CSV_FILE);
        }
    }

    private long getRowCount(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM person")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private void writeBatchToCsv(Connection connection, BufferedWriter writer, int offset, int limit) throws SQLException, IOException {
        String query = "SELECT name, email, address, age FROM person ORDER BY id LIMIT ? OFFSET ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, limit);
            stmt.setInt(2, offset);
            try (ResultSet rs = stmt.executeQuery()) {
                StringBuilder sb = new StringBuilder();
                while (rs.next()) {
                    sb.append(escapeCsv(rs.getString("name"))).append(",");
                    sb.append(escapeCsv(rs.getString("email"))).append(",");
                    sb.append(escapeCsv(rs.getString("address"))).append(",");
                    sb.append(rs.getInt("age")).append("\n");
                }
                synchronized (writer) {
                    writer.write(sb.toString());
                }
            }
        }
    }

    private String escapeCsv(String value) {
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            value = "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}
