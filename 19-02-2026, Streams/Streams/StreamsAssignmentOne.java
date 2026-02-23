package Streams;

import java.util.*;
import java.util.stream.*;

// Find all even numbers from a list of integers
// Find all numbers greater than 50
// Convert add strings in a list to uppercase
// Remove all null values from a list
// Get the length of each string in a list
// Find names starting with the letter 'A'
// Find squares of all numbers
// Filter employees with salary > 50,000
// Get list of emails from list of employees
// Count how many numbers are divisible by 3

class Employee
{
    String name;
    String email;
    double salary;

    Employee(String name, String email, double salary)
    {
        this.name = name;
        this.email = email;
        this.salary = salary;
    }

    public String getName()
    {
        return name;
    }

    public String getEmail()
    {
        return email;
    }

    public double getSalary()
    {
        return salary;
    }

    @Override
    public String toString()
    {
        return name + " - " + salary;
    }
}

public class StreamsAssignmentOne
{
    public static void main(String[] args)
    {
        List<Integer> numbers = Arrays.asList( 10, 23, 50, 60, 75, 90, 3, 6, 9 );
        List<String> strings = Arrays.asList( "apple", "banana", null, "avocado", "cherry", "apricot" );

        List<Employee> employees = Arrays.asList(
                new Employee("Amit", "amit@gmail.com", 60000),
                new Employee("Rahul", "rahul@gmail.com", 45000),
                new Employee("Anita", "anita@gmail.com", 75000),
                new Employee("Karan", "karan@gmail.com", 30000)
        );

        // 1. Find all even numbers
        List<Integer> evenNumbers = numbers.stream()
                .filter(n -> n % 2 == 0)
                .toList();
        System.out.println("Even Numbers: " + evenNumbers);

        // 2. Find numbers greater than 50
        List<Integer> greaterThan50 = numbers.stream()
                .filter(n -> n > 50)
                .toList();
        System.out.println("Numbers > 50: " + greaterThan50);

        // 3. Convert strings to uppercase
        List<String> upperCase = strings.stream()
                .filter(Objects::nonNull)
                .map(String::toUpperCase)
                .toList();
        System.out.println("Uppercase Strings: " + upperCase);

        // 4. Remove null values
        List<String> noNulls = strings.stream()
                .filter(Objects::nonNull)
                .toList();
        System.out.println("Without Nulls: " + noNulls);

        // 5. Length of each string
        List<Integer> lengths = strings.stream()
                .filter(Objects::nonNull)
                .map(String::length)
                .toList();
        System.out.println("Lengths: " + lengths);

        // 6. Names starting with 'A'
        List<String> startsWithA = strings.stream()
                .filter(Objects::nonNull)
                .filter(s -> s.startsWith("a") || s.startsWith("A"))
                .toList();
        System.out.println("Starts with A: " + startsWithA);

        // 7. Squares of all numbers
        List<Integer> squares = numbers.stream()
                .map(n -> n * n)
                .toList();
        System.out.println("Squares: " + squares);

        // 8. Employees with salary > 50,000
        List<Employee> highSalary = employees.stream()
                .filter(e -> e.getSalary() > 50000)
                .toList();
        System.out.println("Salary > 50k: " + highSalary);

        // 9. List of employee emails
        List<String> emails = employees.stream()
                .map(Employee::getEmail)
                .toList();
        System.out.println("Emails: " + emails);

        // 10. Count numbers divisible by 3
        long countDivisibleBy3 = numbers.stream()
                .filter(n -> n % 3 == 0)
                .count();
        System.out.println("Count divisible by 3: " + countDivisibleBy3);
    }
}