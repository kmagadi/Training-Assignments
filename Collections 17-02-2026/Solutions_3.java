import java.util.Scanner;
import java.util.HashMap;
import java.util.Map;

public class Solutions_3 {
    public static void main(String[] args) {
        HashMap< String, Integer > map = new HashMap<>();

        gatherRequirements( map );

        printMapEntries( map );
    }
    private static void gatherRequirements( HashMap< String, Integer > map )
    {
        Scanner sc = new Scanner( System.in );
        System.out.println("Please enter required items! \n Enter 'STOP' to stop entering ;)");
        boolean flag = true;

        while( flag )
        {
            String temp = sc.nextLine();
            if( temp.equals("STOP") )
            {
                flag = false;
            }
            else
            {
                map.put( temp, map.getOrDefault( temp, 0 ) + 1 );
            }
        }
        
        sc.close();
    }
    private static void printMapEntries( HashMap< String, Integer > map )
    {
        System.out.println("The entries are as follows :");
        
        for ( Map.Entry< String, Integer > entry : map.entrySet() ) 
            {
            System.out.println( entry.getKey() + " " + entry.getValue() );
        }
    }
}
