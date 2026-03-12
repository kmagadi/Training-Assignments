import java.util.Scanner;

public class TrappingRainwater
{
    public static int trap(int[] height)
    {
        int left = 0;
        int right = height.length - 1;

        int leftMax = 0;
        int rightMax = 0;

        int water = 0;

        while (left < right)
        {
            if (height[left] < height[right])
            {
                if (height[left] >= leftMax)
                    leftMax = height[left];
                else
                    water += leftMax - height[left];

                left++;

            }
            else
            {
                if (height[right] >= rightMax)
                    rightMax = height[right];
                else
                    water += rightMax - height[right];

                right--;
            }
        }
        return water;
    }

    public static void main(String[] args)
    {
        Scanner sc = new Scanner(System.in);

        System.out.print("Enter number of bars: ");
        int n = sc.nextInt();

        int[] arr = new int[n];

        System.out.println("Enter heights:");

        for (int i = 0; i < n; i++)
            arr[i] = sc.nextInt();

        int result = trap(arr);

        System.out.println("Total trapped water = " + result);

        sc.close();
    }
}