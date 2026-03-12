import java.util.Scanner;

public class GradeCategorizer {
    public static void main(String[] args) {
        Scanner sc = new Scanner( System.in );

        int marks = sc.nextInt(); // You can change this value to test with different marks

        switch ( marks )
        {
            case 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100:
                System.out.println("Grade: A");
                break;
            case 80, 81, 82, 83, 84, 85, 86, 87, 88, 89:
                System.out.println("Grade: B");
                break;
            case 70, 71, 72, 73, 74, 75, 76, 77, 78, 79:
                System.out.println("Grade: C");
                break;
            case 60, 61, 62, 63, 64, 65, 66, 67, 68, 69:
                System.out.println("Grade: D");
                break;
            default:
                System.out.println("Grade: F");
        }

        if (marks >= 90) {
            System.out.println("Grade: A");
        } else if (marks >= 80) {
            System.out.println("Grade: B");
        } else if (marks >= 70) {
            System.out.println("Grade: C");
        } else if (marks >= 60) {
            System.out.println("Grade: D");
        } else {
            System.out.println("Grade: F");
        }

        sc.close();
    }
}