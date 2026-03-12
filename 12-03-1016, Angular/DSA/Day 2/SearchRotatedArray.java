import java.util.Scanner;

public class SearchRotatedArray
{
    public static int search(int[] nums, int target)
    {
        int left = 0;
        int right = nums.length - 1;

        while (left <= right)
        {
            int mid = left + (right - left) / 2;

            if (nums[mid] == target)
                return mid;

            if (nums[left] <= nums[mid])
            {

                if (target >= nums[left] && target < nums[mid])
                    right = mid - 1;
                else
                    left = mid + 1;

            }
            else
            {
                if (target > nums[mid] && target <= nums[right])
                    left = mid + 1;
                else
                    right = mid - 1;
            }
        }

        return -1;
    }

    public static void main(String[] args)
    {
        Scanner sc = new Scanner(System.in);

        System.out.print("Enter array size: ");
        int n = sc.nextInt();

        int[] arr = new int[n];

        System.out.println("Enter elements of rotated sorted array:");
        for (int i = 0; i < n; i++)
            arr[i] = sc.nextInt();

        System.out.print("Enter target: ");
        int target = sc.nextInt();

        int result = search(arr, target);

        System.out.println("Index of target: " + result);

        sc.close();
    }
}