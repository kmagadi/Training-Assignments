import java.util.Scanner;
import java.util.Arrays;

public class DutchNationFlag
{
    public static void main( String[] args )
    {
        Scanner sc = new Scanner( System.in );

        System.out.println("Please enter size of array -> ");
        int size = sc.nextInt();

        int[] arr = new int[ size ];

        System.out.println("Only Enter 0, 1, or 2 in the array!!!!");
        for( int i = 0; i < size; i++ )
        {
            int temp = sc.nextInt();
            if( temp == 0 || temp == 1 || temp == 2 )
                arr[ i ] = temp;
            else
                System.out.println("Enter valid number!!!!");
        }

        dutchFlag( arr );

        System.out.println( Arrays.toString( arr ));
        sc.close();
    }
    public static void dutchFlag( int[] arr )
    {
        int low = 0;
        int mid = 0;
        int high = arr.length - 1;

        while ( mid <= high )
        {
            if ( arr[ mid ] == 0 )
            {
                int temp = arr[ low ];
                arr[ low ] = arr[ mid ];
                arr[ mid ] = temp;

                low++;
                mid++;
            }
            else if ( arr[ mid ] == 1 )
            {
                mid++;
            }
            else
            {
                int temp = arr[ mid ];
                arr[ mid ] = arr[ high ];
                arr[ high ] = temp;

                high--;
            }
        }
    }
}