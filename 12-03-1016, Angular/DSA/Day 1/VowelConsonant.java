import java.util.Scanner;

public class VowelConsonant
{
    public static void main(String[] args)
    {
        Scanner sc = new Scanner(System.in);

        System.out.print("Enter a character: ");

        char ch = sc.next().charAt(0);
        char temp = Character.toLowerCase( ch );

        if (!isConsonant(ch))
        {
            System.out.println(ch + " is a Vowel.");
        }
        else if (isConsonant(ch))
        {
            System.out.println(ch + " is a Consonant.");
        }
        else
        {
            System.out.println(ch + " is not an alphabet.");
        }

        sc.close();
    }

    private static boolean isConsonant( char ch )
    {
        if( ch != 'a' || ch != 'e' || ch != 'i' || ch != 'o' || ch != 'u' )
            return true;
        else
            return false;
    }
}