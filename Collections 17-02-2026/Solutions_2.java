import java.util.ArrayList;
import java.util.Scanner;

public class Solutions_2 {
    public static void main(String[] args) {
        ArrayList< String > refList = new ArrayList<>();
        
        GatherOrderDetails( refList );
        
        printAll( refList );
    }
    private static void printAll( ArrayList< String > refList )
    {
        System.out.println("The elements are: ");
        for( String str : refList )
            {
                System.out.println( str );
            }
    }
    private static void GatherOrderDetails( ArrayList< String > refList )
    {
        Scanner sc = new Scanner(System.in);
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
                    refList.add( temp );
                }
            }
        sc.close();
    }
}