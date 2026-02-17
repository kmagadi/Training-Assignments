import java.util.Scanner;

class ListNode
{
    String data;
    ListNode next;
    ListNode( )
    {
        this.data = "";
        this.next = null;
    }
    ListNode( String data )
    {
        this.data = data;
        this.next = null;
    }
}

public class Solutions_5 {
    public static void main(String[] args) {
        ListNode head = new ListNode("DUMMY_HEADER");
        
        getTaskDetails( head );

        traverseList( head.next );
    }

    private static void getTaskDetails( ListNode head )
    {
        Scanner sc = new Scanner( System.in );
        ListNode tptr = head;
        boolean flag = true;
        System.out.println("Please enter the Task Details below (Type 'STOP' to exit): ");

        while( flag )
        {
            String newStr = sc.nextLine();
            if( newStr.equalsIgnoreCase("STOP") )
            {
                flag = false;
            }
            else
            {
                ListNode nn = new ListNode( newStr );
                tptr.next = nn;
                tptr = tptr.next;
            }
        }

        sc.close();
    }

    private static void traverseList( ListNode head )
    {
        System.out.println("Values are as follows: ");

        while( head != null )
        {
            System.out.println( head.data );
            head = head.next;
        }
    }
}