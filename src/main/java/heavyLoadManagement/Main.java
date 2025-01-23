package heavyLoadManagement;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        LoadCreator loadCreator = new LoadCreator();
        DatabaseInserter databaseInserter = new DatabaseInserter();

        System.out.println("Generating persons...");
        List<Person> persons = loadCreator.createPersons();
        System.out.println("Persons generated. Starting database insertion...");

        databaseInserter.insertPersons(persons);

        System.out.println("Process completed successfully.");
    }
}

