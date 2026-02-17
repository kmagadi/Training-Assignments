import java.util.Scanner;
import java.util.HashMap;
import java.util.Map;

public class Solutions_8 
{
    public static void main(String[] args) 
    {
        HashMap< Integer, String > employeeMap = new HashMap<>();
        
        collectEmployeeData( employeeMap );

        displayEmployees( employeeMap );
    }
    private static void collectEmployeeData( HashMap< Integer, String > employeeMap )
    {
        Scanner sc = new Scanner( System.in );
        boolean flag = true;
        System.out.println("Employee Database System");
        System.out.println("Enter ID and Name (Example: 101 Rahul)");
        System.out.println("Type 'STOP' to finish.");

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
                    int id = Integer.parseInt(input);
                    String name = sc.next();

                    employeeMap.put(id, name);
                } 
                catch ( NumberFormatException e ) 
                {
                    System.out.println("Invalid ID. Please enter a number.");
                }
            }
        }

        sc.close();
    }
    private static void displayEmployees( HashMap< Integer, String > employeeMap )
    {
        System.out.println("\nRecorded Employees:");

        for ( Map.Entry< Integer, String > entry : employeeMap.entrySet() ) 
        {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }
}