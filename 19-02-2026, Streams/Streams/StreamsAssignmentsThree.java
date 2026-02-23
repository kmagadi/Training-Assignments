package Streams;

import java.util.*;
import java.util.stream.*;

class Employee {
    String name;
    String dept;
    String email;
    int age;
    double salary;

    Employee(String name, String dept, String email, int age, double salary) {
        this.name = name;
        this.dept = dept;
        this.email = email;
        this.age = age;
        this.salary = salary;
    }

    public String getDept() { return dept; }
    public double getSalary() { return salary; }
    public int getAge() { return age; }

    @Override
    public String toString() {
        return name + " - " + salary;
    }
}

public class StreamsAssignmentsThree {

    public static void main(String[] args) {

        List<Integer> numbers = Arrays.asList(10, 200, 50, 99, 150, 300, 5, 600);

        List<Employee> employees = Arrays.asList(
                new Employee("Amit", "IT", "a@gmail.com", 25, 40000),
                new Employee("Rahul", "HR", "r@gmail.com", 35, 120000),
                new Employee("Anita", "IT", "an@gmail.com", 28, 80000),
                new Employee("Karan", "Finance", "k@gmail.com", 17, 25000)
        );

        // 21. Max number
        System.out.println("Max: " +
                numbers.stream().max(Integer::compareTo).get());

        // 22. Min number
        System.out.println("Min: " +
                numbers.stream().min(Integer::compareTo).get());

        // 23. Sum
        System.out.println("Sum: " +
                numbers.stream().reduce(0, Integer::sum));

        // 24. Average
        System.out.println("Average: " +
                numbers.stream().mapToInt(i -> i).average().getAsDouble());

        // 25. All employees salary > 30k
        boolean allAbove30k = employees.stream()
                .allMatch(e -> e.getSalary() > 30000);
        System.out.println("All > 30k: " + allAbove30k);

        // 26. Any employee salary > 1 lakh
        boolean anyAbove1L = employees.stream()
                .anyMatch(e -> e.getSalary() > 100000);
        System.out.println("Any > 1L: " + anyAbove1L);

        // 27. No employee under 18
        boolean noneUnder18 = employees.stream()
                .noneMatch(e -> e.getAge() < 18);
        System.out.println("None under 18: " + noneUnder18);

        // 28. First element > 100
        System.out.println("First > 100: " +
                numbers.stream().filter(n -> n > 100).findFirst().get());

        // 29. Any employee from IT
        System.out.println("Employee IT: " +
                employees.stream()
                        .filter(e -> e.getDept().equals("IT"))
                        .findAny().get());

        // 30. Convert to array
        Integer[] arr = numbers.stream().toArray(Integer[]::new);
        System.out.println("Array: " + Arrays.toString(arr));
    }
}
