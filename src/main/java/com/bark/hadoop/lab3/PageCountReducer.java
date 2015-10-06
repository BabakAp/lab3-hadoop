/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author roozbeh
 */
public class PageCountReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        int sum = 0;
//        for (IntWritable v : values) {
//            sum += v.get();
//        }
//        result.set(sum);
        Text text = new Text("alaki");
        context.write(key, text);
    }
}
