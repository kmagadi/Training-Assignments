import java.util.function.Predicate;

public class StreamsDemo
{
    public static void main( String[] args )
    {
        Predicate < Integer > predicate = new Predicate< Integer >() {
            @Override
            public boolean test( Integer integer )
            {
                return integer.intValue() % 2 == 0;
            }   
        };

        System.out.println( predicate.test( 34 ) );
    }
}