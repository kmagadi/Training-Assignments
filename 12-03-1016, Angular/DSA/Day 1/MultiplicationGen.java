import java.util.List;
import java.util.Scanner;

public class MultiplicationGen {

    public static List<Integer> generateMultiplicationTable(int n) {
        return java.util.stream.IntStream.rangeClosed(1, 10)
                .map(i -> i * n)
                .boxed()
                .toList();
    }

    public static void main(String[] args)
    {
        Scanner sc = new Scanner( System.in );
        int number = sc.nextInt();

        List< Integer > multiplicationTable = generateMultiplicationTable( number );

        System.out.println( "Multiplication Table for " + number + ": " + multiplicationTable );
    }
}