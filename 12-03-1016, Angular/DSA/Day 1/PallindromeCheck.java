import java.util.Scanner;

public class PallindromeCheck
{
    public static void main(String[] args)
    {
        Scanner sc = new Scanner( System.in );

        System.out.print("Enter a number: ");
        int num = sc.nextInt();

        int original = num;
        int reversed = 0;

        while (num != 0)
        {
            int digit = num % 10;
            reversed = reversed * 10 + digit;
            num = num / 10;
        }

        System.out.println("Reversed number: " + reversed);

        if (original == reversed)
            System.out.println("It is a Palindrome");
        else
            System.out.println("Not a Palindrome");

        sc.close();
    }
}