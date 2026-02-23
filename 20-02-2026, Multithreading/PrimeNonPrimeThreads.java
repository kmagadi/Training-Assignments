public class PrimeNonPrimeThreads
{
    public static void main( String[] args )
    {
        NumberPrinter printer = new NumberPrinter( 20 );

        Thread primeThread = new Thread(() -> printer.printPrimes(), "Prime-Thread");
        Thread nonPrimeThread = new Thread(() -> printer.printNonPrimes(), "NonPrime-Thread");

        primeThread.start();
        nonPrimeThread.start();
    }
}

class NumberPrinter
{
    private final int limit;
    private boolean isPrimeTurn = true;

    public NumberPrinter(int limit)
    {
        this.limit = limit;
    }

    public synchronized void printPrimes()
    {
        int current = 2;
        for ( int i = 0; i < limit; i++ )
        {
            while ( !isPrimeTurn )
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
            while ( !isPrime( current ))
            {
                current++;
            }

            System.out.println( Thread.currentThread().getName() + ": " + current++ );
            isPrimeTurn = false;
            notifyAll();
        }
    }

    public synchronized void printNonPrimes()
    {
        int current = 1;
        for ( int i = 0; i < limit; i++ )
        {
            while ( isPrimeTurn )
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
            while ( isPrime( current ))
            {
                current++;
            }

            System.out.println( Thread.currentThread().getName() + ": " + current++ );
            isPrimeTurn = true;
            notifyAll();
        }
    }

    private boolean isPrime( int n )
    {
        if ( n <= 1 ) return false;
        for ( int i = 2; i <= Math.sqrt( n ); i++ )
        {
            if ( n % i == 0 ) return false;
        }
        return true;
    }
}