public class OddEvenThreads
{
    public static void main( String[] args )
    {
        Printer printer = new Printer( 20 );

        Thread oddThread = new Thread(() -> printer.printOdd(), "Odd-Thread" );
        Thread evenThread = new Thread(() -> printer.printEven(), "Even-Thread" );

        oddThread.start();
        evenThread.start();
    }
}

class Printer
{
    private int counter = 1;
    private final int limit;

    public Printer(int limit)
    {
        this.limit = limit;
    }

    public synchronized void printOdd()
    {
        while ( counter <= limit )
        {
            while ( counter % 2 == 0 )
            {
                try
                {
                    wait();
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                }
            }
            if ( counter <= limit )
            {
                System.out.println( Thread.currentThread().getName() + ": " + counter );
                counter++;
                notifyAll();
            }
        }
    }

    public synchronized void printEven()
    {
        while ( counter <= limit )
        {
            while ( counter % 2 != 0 )
            {
                try { wait(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            if ( counter <= limit )
            {
                System.out.println( Thread.currentThread().getName() + ": " + counter );
                counter++;
                notifyAll();
            }
        }
    }
}