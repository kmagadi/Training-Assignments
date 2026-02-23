public class FourThreadSequence
{
    public static void main( String[] args )
    {
        SequencePrinter printer = new SequencePrinter( 200 );

        Thread tA = new Thread(() -> printer.print(1), "Thread-A" );
        Thread tB = new Thread(() -> printer.print(2), "Thread-B" );
        Thread tC = new Thread(() -> printer.print(3), "Thread-C" );
        Thread tD = new Thread(() -> printer.print(0), "Thread-D" );

        tA.start(); tB.start(); tC.start(); tD.start();
    }
}

class SequencePrinter
{
    private int counter = 1;
    private final int limit;

    public SequencePrinter( int limit )
    {
        this.limit = limit;
    }

    public synchronized void print( int threadId )
    {
        while ( counter <= limit )
        {
            while ( counter % 4 != threadId && counter <= limit )
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
}