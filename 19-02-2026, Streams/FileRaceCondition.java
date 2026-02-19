import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class FileRaceCondition 
{
    public static void main(String[] args) 
    {
        String fileName = "output.txt";

        // Thread 1: Writes "APPLE" 1000 times
        Runnable task1 = () -> writeToFile(fileName, "APPLE ");

        // Thread 2: Writes "ORANGE" 1000 times
        Runnable task2 = () -> writeToFile(fileName, "ORANGE ");

        Thread t1 = new Thread(task1);
        Thread t2 = new Thread(task2);

        System.out.println("Starting threads!");

        try{
            t1.start();
        
            t1.join();
            // Ensure that the JVM waits here until t1 is done running, 
            //      we can also use synchronized keyword in the writeToFile function but it 
        
            t2.start();
        }
        catch( Exception e )
        {
            System.out.println( e );
        }
        finally {
            System.out.println("Done executing!!!");
        }
    }

    private static void writeToFile(String fileName, String content) 
    {
        // We use 'true' for append mode
        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName, true))) 
        {
            for (int i = 0; i < 1000; i++) 
                {
                writer.print(content);
                // A tiny sleep can actually make the race condition more likely
                // as it forces thread context switching.
            }
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        }
    }
}