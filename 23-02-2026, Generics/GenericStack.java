public class GenericStack<T>
{
    T[] array;
    int currPtr;

    @SuppressWarnings("unchecked")
    GenericStack( int size )
    {
        array = (T[]) new Object[ size ];
        currPtr = 0;
    }
    public void push( T value )
    {
        array[ currPtr ] = value;
        currPtr++;
    }

    public T pop()
    {
        currPtr--;
        T result = array[ currPtr ];

        array[ currPtr ] = null;

        return result;
    }

    public T peek()
    {
        return array[ currPtr - 1 ];
    }

    public boolean isEmpty()
    {
        return currPtr == 0 ? true : false;
    }

    private void printEle()
    {
        for( int i = 0; i < currPtr; i++ )
        {
            System.out.print( array[ i ] + " " );
        }
        System.out.println();
    }

    public static void main(String[] args)
    {
        GenericStack<Integer> stack = new GenericStack<>( 10 );

        for( int i = 0; i < 5; i++ )
        {
            stack.push( i * 90 );
        }

        stack.printEle();
    }
}