import java.util.*;
import java.util.stream.*;

public class StreamsAssignment {
    public static void main(String[] args) {

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);

        // First Question, filter through even numbers
        numbers.stream()
        .filter(n -> n % 2 != 0)   // lambda to filter even
       .forEach(System.out::println);

       System.out.println("___________________________________________________________________");
       
        // Convert all String to uppercase
        List< String > strings = Arrays.asList("hi", "hello", "bye", "hallo");

        strings.stream()
        .map(s -> s.toUpperCase())   // convert to uppercase
        .forEach(System.out::println);
    }
}
