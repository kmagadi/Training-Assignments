import java.util.Scanner;
import java.util.ArrayList;
import java.util.Collections;

public class Solutions_9 
{
    public static void main(String[] args) 
    {
        ArrayList< String > students = new ArrayList<>();
        
        collectNames( students );

        sortAndPrint( students );
    }

    private static void collectNames( ArrayList< String > students )
    {
        Scanner sc = new Scanner( System.in );
        boolean flag = true;
        System.out.println("Enter Student Names (Type 'STOP' to sort):");

        while( flag )
        {
            String name = sc.nextLine();
            if( name.equalsIgnoreCase("STOP") )
            {
                flag = false;
            }
            else
            {
                students.add( name );
            }
        }
    }
    private static void sortAndPrint( ArrayList<String> students )
    {
        Collections.sort( students );
        
        System.out.println("\nSorted Names:");
        for( String name : students )
        {
            System.out.println( name );
        }
    }
}