package heavyLoadManagement;
import com.github.javafaker.Faker;
import java.util.ArrayList;
import java.util.List;

public class LoadCreator {
    private static final int TOTAL_RECORDS = 10000000;

    public List<Person> createPersons() {
        Faker faker = new Faker();
        List<Person> persons = new ArrayList<>();

        for (int i = 0; i < TOTAL_RECORDS; i++) {
            String name = faker.name().fullName();
            String email = faker.internet().emailAddress();
            String address = faker.address().fullAddress();
            int age = faker.number().numberBetween(18, 100);
            persons.add(new Person(name, email, address, age));
        }

        return persons;
    }
}
