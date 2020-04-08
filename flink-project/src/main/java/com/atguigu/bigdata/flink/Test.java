package com.atguigu.bigdata.flink;

import java.util.Arrays;

public class Test {
    public static void main(String[] args) {
        int[] data = {43,45,12,98,222};
        bubbleSort(data);
    }

////    private static String boolleSort(Integer[] a) {
////        int i;
////        int j;
////        for(i=0;i<a.length-1;i++){
////            for(j=0;j<a.length-1-i;j++){
////                if(a[j]>a[j+1]){
////                    int temp = a[j];
////                    a[j]=a[j+1];
////                    a[j+1]=temp;
////                }
////            }
////        }
//
//        return Arrays.toString(a);
//    }


    public static void bubbleSort(int[] data) {

        System.out.println("开始排序");
        int arrayLength = data.length;
        for (int i = 0; i < arrayLength - 1; i++) {
            boolean flag = false;
            for (int j = 0; j < arrayLength - 1 - i; j++) {
                if(data[j] > data[j + 1]){
                    int temp = data[j + 1];
                    data[j + 1] = data[j];
                    data[j] = temp;
                    flag = true;
                }
            }
            System.out.println(java.util.Arrays.toString(data));

            if (!flag)
                break;
        }
    }
    private static void douFen(Integer[] arr,Integer find){
        int left=arr[0];
        int right=arr[arr.length-1];
        int zhongjian=arr[(left+right)/2];
        while(left<right){
            if(arr[find]<arr[zhongjian]){
                arr[zhongjian]=arr[zhongjian-1];
            }else if(arr[find]>arr[zhongjian]){
                arr[zhongjian]=arr[zhongjian+1];
            }else{
                arr[zhongjian]=arr[find];
                break;
            }
        }

    }

}

