import java.util.Scanner;
import java.util.PriorityQueue;
import java.util.Collections;

public class Solutions_6 
{
    public static void main(String[] args) 
    {
        PriorityQueue< Integer > maxHeap = new PriorityQueue<>( Collections.reverseOrder() );
        
        getNumbers( maxHeap );

        printHighest( maxHeap );
    }

    private static void getNumbers( PriorityQueue< Integer > maxHeap )
    {
        Scanner sc = new Scanner( System.in );
        boolean flag = true;
        System.out.println("Please enter numbers (Type 'STOP' to see the highest): ");

        while( flag )
        {
            String input = sc.next();
            if( input.equalsIgnoreCase("STOP") )
            {
                flag = false;
            }
            else
            {
                try 
                {
                    int num = Integer.parseInt(input);
                    maxHeap.add(num);
                } 
                catch (NumberFormatException e) 
                {
                    System.out.println("Please enter a valid number or 'STOP'.");
                }
            }
        }
        sc.close();
    }

    private static void printHighest( PriorityQueue<Integer> maxHeap )
    {
        if ( maxHeap.isEmpty() ) 
        {
            System.out.println("No numbers were entered.");
        } 
        else 
            {
            System.out.println("The highest number is: " + maxHeap.peek());
            
            System.out.println("All numbers in descending order:");
            
            while ( !maxHeap.isEmpty() ) 
            {
                System.out.print(maxHeap.poll() + " ");
            }
        }
    }
}