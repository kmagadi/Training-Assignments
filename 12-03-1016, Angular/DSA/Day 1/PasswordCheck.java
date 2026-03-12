import java.util.Scanner;

public class PasswordCheck
{
    public static void main(String[] args)
    {
        Scanner sc = new Scanner( System.in );

        String secretPassword = "bounteous x accolite";
        String userInput;

        do
        {
            System.out.print("Enter the password: ");
            userInput = sc.nextLine();
        }
        while( !userInput.equals( secretPassword ));

        System.out.println("Access Granted!!");

        sc.close();
    }
}