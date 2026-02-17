import java.util.Scanner;
import java.util.HashSet;

public class Solutions_4 {
    public static void main(String[] args) {
        HashSet< Integer > set = new HashSet<>();

        getNumericData( set );

        printAll( set );
    }   
    private static void getNumericData( HashSet< Integer > set )
    {
        Scanner sc = new Scanner( System.in );
        System.out.println("Please enter the numbers: ");
        boolean flag = true;

        System.out.println("Please enter '-1' to end entering names!");
        while( flag )
        {
            Integer newInt = sc.nextInt();
            if( newInt == -1 )
            {
                flag = false;
            }
            else
            {
                set.add( newInt );
            }
        }

        sc.close();
    }
    private static void printAll( HashSet< Integer > set )
    {
        System.out.println("Result set is as follows: ");
        for( Integer num : set )
        {
            System.out.println( num );
        }
    }
}