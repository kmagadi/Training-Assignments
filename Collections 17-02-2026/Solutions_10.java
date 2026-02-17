import java.util.Scanner;
import java.util.HashSet;
import java.util.ArrayList;

public class Solutions_10 
{
    public static void main(String[] args) {
        ArrayList< Integer > numbers = new ArrayList<>();
        
        collectNumbers( numbers );

        findDuplicates( numbers );
    }
    private static void collectNumbers( ArrayList< Integer > numbers )
    {
        Scanner sc = new Scanner( System.in );
        System.out.println("Enter numbers separated by spaces (e.g., 1 2 3 2 1):");
        System.out.println("Type 'STOP' when finished.");

        while( sc.hasNextInt() )
        {
            numbers.add( sc.nextInt() );
        }

        sc.close();
    }
    private static void findDuplicates( ArrayList< Integer > numbers )
    {
        HashSet< Integer > seen = new HashSet<>();
        HashSet< Integer > duplicates = new HashSet<>();

        for( Integer num : numbers )
        {
            if( !seen.add( num ) )
            {
                duplicates.add( num );
            }
        }

        System.out.println("\nDuplicates: " + duplicates);
    }
}