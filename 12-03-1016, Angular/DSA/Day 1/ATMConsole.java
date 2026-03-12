import java.util.Scanner;

public class ATMConsole
{
    public static void main(String[] args)
    {
        int balance = 1000;
        Scanner sc = new Scanner( System.in );

        String userInput;

        while( true )
        {
            System.out.println("Welcome to the ATM!!");
            System.out.println("1. Check Balance");
            System.out.println("2. Withdraw");
            System.out.println("3. Deposit");
            System.out.println("4. Exit");

            userInput = sc.nextLine();

            switch ( userInput )
            {
                case "1":
                    System.out.println("Your current balance is: $" + balance);
                    break;
                case "2":
                    System.out.print("Enter amount to withdraw -> ");
                    int withdrawAmount = sc.nextInt();
                    sc.nextLine();
                    if (withdrawAmount > balance) {
                        System.out.println("Insufficient funds!");
                    } else {
                        balance -= withdrawAmount;
                        System.out.println("You have withdrawn: $" + withdrawAmount);
                        System.out.println("Your new balance is: $" + balance);
                    }
                    break;
                case "3":
                    System.out.print("Enter amount to deposit -> ");
                    int depositAmount = sc.nextInt();
                    sc.nextLine();
                    balance += depositAmount;
                    System.out.println("You have deposited: $" + depositAmount);
                    System.out.println("Your new balance is: $" + balance);
                    break;
                case "4":
                    System.out.println("Thank you for using the ATM Console. Goodbye!");
                    sc.close();
                    return;
                default:
                    System.out.println("Invalid choice! Please try again.");
            }

            System.out.println();
        }
    }
}