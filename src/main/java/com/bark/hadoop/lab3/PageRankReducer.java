/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        int sum = 0;
//
//        for (IntWritable value : values) {
//            sum += value.get();
//        }
//
//        context.write(key, new IntWritable(sum));
    }
    //TODO: Move this to main class.
    //conf.set("mapred.textoutputformat.separator", "=");
}
