import java.util.Scanner;
import java.util.LinkedList;
import java.util.Queue;

public class Solutions_7 
{
    public static void main(String[] args) 
    {
        Queue< String > patientQueue = new LinkedList<>();
        
        registerPatients( patientQueue );

        servePatients( patientQueue );
    }

    private static void registerPatients( Queue< String > patientQueue )
    {
        Scanner sc = new Scanner( System.in );
        boolean flag = true;
        System.out.println("Hospital Registration System");
        System.out.println("Enter Patient Name (Type 'STOP' to start treatment): ");

        while( flag )
        {
            String name = sc.nextLine();
            if( name.equalsIgnoreCase("STOP") )
            {
                flag = false;
            }
            else
            {
                patientQueue.add( name );
            }
        }
    }

    private static void servePatients( Queue< String > patientQueue )
    {
        System.out.println("\nDoctor is now ready:");

        while( !patientQueue.isEmpty() )
        {
            String currentPatient = patientQueue.poll();
            System.out.println("Serving: " + currentPatient);
        }
        
        System.out.println("All patients have been treated.");
    }
}