/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

//    private IntWritable result = new IntWritable();
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        int sum = 0;
//        for (IntWritable v : values) {
//            sum += v.get();
//        }
//        result.set(sum);
        boolean isRedLink = true;
        for (Text t : values) {
            if (t.toString().equalsIgnoreCase("!")) {
                isRedLink = false;
            }
        }
        if (!isRedLink) {
            for (Text t : values) {
                context.write(key, t);
            }
        }
    }
}
