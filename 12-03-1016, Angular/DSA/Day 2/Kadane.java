import java.util.Scanner;

public class Kadane
{
    public static void main(String[] args)
    {
        Scanner sc = new Scanner( System.in );

        System.out.println("Please enter size of array -> ");
        int size = sc.nextInt();

        int[] arr = new int[ size ];

        System.out.println("Kindly enter elements to be added in the array!!!!");
        for( int i = 0; i < size; i++ )
        {
            arr[ i ] = sc.nextInt();
        }
        sc.close();

        System.out.println( Kadane( arr ));
    }
    public static int Kadane( int[] arr )
    {
        int maxSum = Integer.MIN_VALUE;
        int currSum = arr[ 0 ];

        for( int i = 1; i < arr.length; i++ )
        {
            currSum = Math.max( arr[ i ], currSum + arr[ i ] );
            maxSum = Math.max( maxSum, currSum );
        }

        return maxSum;
    }
}