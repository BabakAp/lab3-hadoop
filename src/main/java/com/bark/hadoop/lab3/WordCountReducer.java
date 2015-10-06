/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

/**
 * Credits:
 * http://log.malchiodi.com/2014/11/12/executing-jar-encoded-mapreduce-jobs-in-aws-either-through-web-interface-or-cli/
 */
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

//    private IntWritable result = new IntWritable();
//    Reduce step can be called multiple times, use a static map to keep track of titles seen?
//    public static HashMap<String, HashSet<String>> adjacencyGraph = new HashMap<>();

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
