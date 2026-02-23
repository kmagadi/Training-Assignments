public class Box<T>
{
    private T value;

    public void set( T value )
    {
        this.value = value;
    }

    public T get()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return String.valueOf( value );
    }

    public static void main( String[] args )
    {
        Box<String> box1 = new Box<>();
        box1.set( "Hello" );

        Box<Integer> box2 = new Box<>();
        box2.set( 100 );

        System.out.println( box1.get() );
        System.out.println( box2.get() );

        System.out.println( "Box 1 toString: " + box1.toString() );
        System.out.println( "Box 2 toString: " + box2.toString() );
    }
}