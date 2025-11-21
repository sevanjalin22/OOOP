import java.util.*;

public class Main {
    public static void main(String[] args) {

        // ------------------- SORTING TESTS -------------------
        int[] arr1 = {5, 1, 8, 3};
        SortingUtil.bubbleSort(arr1);
        System.out.println("Bubble Sorted: " + Arrays.toString(arr1));

        int[] arr2 = {10, 3, 1, 2};
        SortingUtil.insertionSort(arr2);
        System.out.println("Insertion Sorted: " + Arrays.toString(arr2));

        int[] arr3 = {7, 4, 1, 3};
        SortingUtil.mergeSort(arr3, 0, arr3.length - 1);
        System.out.println("Merge Sorted: " + Arrays.toString(arr3));

        int[] arr4 = {44, 11, 33, 22};
        SortingUtil.quickSort(arr4, 0, arr4.length - 1);
        System.out.println("Quick Sorted: " + Arrays.toString(arr4));


        // ------------------- SEARCHING TESTS -------------------
        int[] s1 = {12, 45, 67, 89};
        System.out.println("Linear Search (67): " + SearchUtil.linearSearch(s1, 67));

        int[] s2 = {1, 3, 5, 7, 9};
        System.out.println("Binary Search (7): " + SearchUtil.binarySearch(s2, 7));


        // ------------------- COLLECTION TESTS -------------------
        Stack<Integer> stack = new Stack<>();
        stack.push(10);
        stack.push(20);
        System.out.println("Stack: " + stack);
        System.out.println("Popped: " + stack.pop());
        System.out.println("Stack after pop: " + stack);

        Queue<String> queue = new LinkedList<>();
        queue.offer("A");
        queue.offer("B");
        System.out.println("Queue: " + queue);
        System.out.println("Polled: " + queue.poll());
        System.out.println("Queue after poll: " + queue);


        // ------------------- TIMER TEST -------------------
        AnalysisUtil.measureTime(() -> SortingUtil.bubbleSort(arr1));
    }
}
