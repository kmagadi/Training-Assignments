public class MaxFinder
{
    public static <T extends Comparable<T>> T findMax( T a, T b, T c )
    {
        T max = a;

        if ( b.compareTo( max ) > 0 )
        {
            max = b;
        }

        if ( c.compareTo( max ) > 0 )
        {
            max = c;
        }

        return max;
    }

    public static void main( String[] args )
    {
        System.out.println( findMax( 10, 20, 5 ) );           // Output: 20

        System.out.println( findMax( "A", "Z", "M" ) );       // Output: Z

        System.out.println( findMax( 3.14, 9.81, 2.71 ) );    // Output: 9.81
    }
}