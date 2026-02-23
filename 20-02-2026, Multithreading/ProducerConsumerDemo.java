import java.util.LinkedList;

public class ProducerConsumerDemo
{
    public static void main( String[] args )
    {
        Buffer buffer = new Buffer( 5 );

        Thread producer = new Thread(() ->
        {
            try
            {
                for ( int i = 1; i <= 15; i++ )
                {
                    buffer.produce( i );
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
        }, "Producer"
        );

        Thread consumer = new Thread(() ->
        {
            try
            {
                for ( int i = 1; i <= 15; i++ )
                {
                    buffer.consume();
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
        }, "Consumer" );

        producer.start();
        consumer.start();
    }
}

class Buffer
{
    private final LinkedList< Integer > list = new LinkedList<>();
    private final int capacity;

    public Buffer( int capacity )
    {
        this.capacity = capacity;
    }

    public synchronized void produce( int value ) throws InterruptedException
    {

        while ( list.size() == capacity )
        {
            System.out.println("Buffer full, Producer is waiting...");
            wait();
        }

        list.add(value);
        System.out.println( Thread.currentThread().getName() + " produced: " + value );
        notifyAll();
        Thread.sleep(100);
    }

    public synchronized void consume() throws InterruptedException
    {
        while ( list.size() == 0 )
        {
            System.out.println( "Buffer empty, Consumer is waiting..." );
            wait();
        }

        int value = list.removeFirst();
        System.out.println( Thread.currentThread().getName() + " consumed: " + value );
        notifyAll();
        Thread.sleep(150);
    }
}