import com.github.javafaker.Faker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ManageHeavyLoad {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/heavyload";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "mupro";
    private static final int BATCH_SIZE = 100000;
    private static final int TOTAL_RECORDS = 10000000;

    public static void main(String[] args) {
        Faker faker = new Faker();

        // Start timing
        long startTime = System.nanoTime();

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            connection.setAutoCommit(false);

            String insertSQL = "INSERT INTO person(name, email, address, age) VALUES (?, ?, ?, ?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {

                for (int i = 1; i <= TOTAL_RECORDS; i++) {
                    String name = faker.name().fullName();
                    String email = faker.internet().emailAddress();
                    String address = faker.address().fullAddress();
                    int age = faker.number().numberBetween(18, 100);

                    preparedStatement.setString(1, name);
                    preparedStatement.setString(2, email);
                    preparedStatement.setString(3, address);
                    preparedStatement.setInt(4, age);

                    preparedStatement.addBatch();

                    if (i % BATCH_SIZE == 0) {
                        preparedStatement.executeBatch();
                        connection.commit();
                        System.out.println("Inserted " + i + " records so far.");
                    }
                }

                // Insert remaining records
                preparedStatement.executeBatch();
                connection.commit();
                System.out.println("All records inserted successfully.");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        // End timing
        long endTime = System.nanoTime();

        // Calculate time taken
        long durationInMillis = (endTime - startTime) / 1_000_000;
        System.out.println("Time taken to insert " + TOTAL_RECORDS + " records: " + durationInMillis + " ms");
    }
}
