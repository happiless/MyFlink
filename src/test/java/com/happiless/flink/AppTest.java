//package com.happiless.flink;
//
//import static org.junit.Assert.assertTrue;
//
//import org.junit.Test;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//
///**
// * Unit test for simple App.
// */
//public class AppTest
//{
//    /**
//     * Rigorous Test :-)
//     */
//    @Test
//    public void shouldAnswerWithTrue()
//    {
//        assertTrue( true );
//    }
//
//    //暑假总天数 5
//    //每份试卷耗时[1,2,3]
//    //每份试卷的价值[1,2,3]
//    //总试卷的份数 3
//    //最大价值
//    @Test
//    public void test(){
//        int total_day = 5;
//        int[] loss = new int[]{1,2,3};
//        double[] values = new double[]{1.0,1.0,2.0};
//        int total_test = 3;
//        int total = 0;
//        int loss_day = 0;
//        int loss_sum = 0;
//        int value_sum = 0;
//        double[] value_loss = new double[loss.length];
//        for (int i = 0; i < loss.length; i++) {
//            loss_sum += loss[i];
//            value_sum += values[i];
//            value_loss[i] = values[i] / loss[i];
//        }
//
//        for (int i = 0; i<value_loss.length; i++) {
//            if(i+1 < value_loss.length && value_loss[i] < value_loss[i+1] ){
//                double temp = value_loss[i];
//                value_loss[i] = value_loss[i+1];
//                value_loss[i+1] = temp;
//                int t1 = loss[i];
//                loss[i] = loss[i+1];
//                loss[i+1] = t1;
//                double t2 = values[i];
//                values[i] = values[i+1];
//                values[i+1] = t2;
//            }
//        }
//        for (int i =0; i<loss.length; i++) {
//            System.out.println(value_loss[i]);
//            System.out.println(loss[i]);
//            System.out.println(values[i]);
//        }
//
//        double total_value = 0;
//        int test_num = 0;
//        for(int i=0; i<loss.length; i++){
//            loss_day += loss[i];
//            if(loss_day > total_day){
//                loss_day -= loss[i];
//                break;
//            }
//            total_value += values[i];
//            test_num = i;
//        }
//
//        double shenyu = total_day - loss_day;
//        if(shenyu > 0 && test_num < total_test){
//            total_value += shenyu / loss[test_num];
//        }
//        System.out.println(total_value);
//    }
//
//
//    @Test
//    public void test1(){
//
//        //  5 3
//        //  14 14 100 3
//        //  76 5 100 3
//        //  78 23 23 3
//        //  45 75 53 3
//        //  52 43 71 3
//        //  5  5  5 125
//        int m = 5;
//        int n = 3;
//        int[][] x = new int[5][3];
//        x[0] = new int[]{14, 14, 100};
//        x[1] = new int[]{76, 5, 100};
//        x[2] = new int[]{78, 23, 23};
//        x[3] = new int[]{45, 75, 53};
//        x[4] = new int[]{52, 43, 71};
//        List<Integer> result = new ArrayList<>();
//        for (int i = 0; i<125; i++){
//            int sum = 0;
//            for(int j=0; j<5; j++){
//                for(int k=j; j<3; k++){
//                    sum += x[j][k];
//                }
//            }
//        }
//        int sum = 0;
//        for(int i=0; i< m; i++){
//            for(int j=i; j<n; j++){
//                sum += x[i][j];
//            }
//        }
//
//    }
//
//}
