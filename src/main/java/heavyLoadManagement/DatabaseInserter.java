package heavyLoadManagement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class DatabaseInserter {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/heavyload";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "mupro";
    private static final int BATCH_SIZE = 10000;

    public void insertPersons(List<Person> persons) {
        String insertSQL = "INSERT INTO load (name, email, address, age) VALUES (?, ?, ?, ?)";

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            connection.setAutoCommit(false);

            try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
                int count = 0;

                for (Person person : persons) {
                    preparedStatement.setString(1, person.getName());
                    preparedStatement.setString(2, person.getEmail());
                    preparedStatement.setString(3, person.getAddress());
                    preparedStatement.setInt(4, person.getAge());
                    preparedStatement.addBatch();

                    count++;
                    if (count % BATCH_SIZE == 0) {
                        preparedStatement.executeBatch();
                        connection.commit();
                        System.out.println("Inserted " + count + " records so far.");
                    }
                }

                // Insert remaining records
                preparedStatement.executeBatch();
                connection.commit();
                System.out.println("All records inserted successfully.");
            }
        } catch (SQLException e) {
            System.out.println("Error inserting records: " + e.getMessage());
        }
    }
}
