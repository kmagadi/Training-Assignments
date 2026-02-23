package Streams;
import java.util.*;
import java.util.stream.*;

class Employee2 {
    String name;
    String email;
    double salary;

    Employee2(String name, String email, double salary) {
        this.name = name;
        this.email = email;
        this.salary = salary;
    }

    public String getName() { return name; }
    public String getEmail() { return email; }
    public double getSalary() { return salary; }

    @Override
    public String toString() {
        return name + " - " + salary;
    }
}

public class StreamsAssignmentsTwo {

    public static void main(String[] args) {

        List<Integer> numbers = Arrays.asList(10, 5, 20, 30, 40, 50, 60, 70, 80, 90, 5, 10);

        List<Employee2> employees = Arrays.asList(
                new Employee2("Amit", "a@gmail.com", 60000),
                new Employee2("Rahul", "r@gmail.com", 45000),
                new Employee2("Anita", "an@gmail.com", 75000),
                new Employee2("Karan", "k@gmail.com", 90000),
                new Employee2("Rahul", "r@gmail.com", 45000)
        );

        // 11. Ascending sort
        System.out.println(numbers.stream().sorted().toList());

        // 12. Descending sort
        System.out.println(numbers.stream()
                .sorted(Comparator.reverseOrder())
                .toList());

        // 13. Sort employees by salary
        System.out.println(employees.stream()
                .sorted(Comparator.comparing(Employee2::getSalary))
                .toList());

        // 14. Sort by name then salary
        System.out.println(employees.stream()
                .sorted(Comparator.comparing(Employee2::getName)
                        .thenComparing(Employee2::getSalary))
                .toList());

        // 15. Remove duplicates
        System.out.println(numbers.stream().distinct().toList());

        // 16. Remove duplicate employees by email
        System.out.println(
                employees.stream()
                        .collect(Collectors.toMap(
                                Employee2::getEmail,
                                e -> e,
                                (e1, e2) -> e1))
                        .values()
        );

        // 17. 3 highest numbers
        System.out.println(numbers.stream()
                .sorted(Comparator.reverseOrder())
                .limit(3)
                .toList());

        // 18. 5 smallest
        System.out.println(numbers.stream()
                .sorted()
                .limit(5)
                .toList());

        // 19. Skip first 5
        System.out.println(numbers.stream()
                .skip(5)
                .toList());

        // 20. Second highest salary
        double secondHighest = employees.stream()
                .sorted(Comparator.comparing(Employee2::getSalary).reversed())
                .skip(1)
                .findFirst()
                .get()
                .getSalary();

        System.out.println("Second highest salary: " + secondHighest);
    }
}
