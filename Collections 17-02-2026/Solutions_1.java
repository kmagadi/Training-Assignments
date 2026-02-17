import java.util.Scanner;
import java.util.HashSet;

public class Solutions_1
{
    public static void main(String[] args) {
        HashSet< String > set = new HashSet<>();

        getStudentData( set );

        printAll( set );
    }
    private static void getStudentData( HashSet< String > set )
    {
        Scanner sc = new Scanner( System.in );
        System.out.println("Please enter the student names: ");
        boolean flag = true;

        System.out.println("Please enter 'STOP' to end entering names!");
        while( flag )
        {
            String newStr = sc.nextLine();
            if( newStr.equals("STOP") )
            {
                flag = false;
            }
            else
            {
                set.add( newStr );
            }
        }

        sc.close();
    }
    private static void printAll( HashSet< String > set )
    {
        for( String temp : set )
        {
            System.out.println("The students who attended the event are: ");
            System.out.println( temp );
        }
    }
}