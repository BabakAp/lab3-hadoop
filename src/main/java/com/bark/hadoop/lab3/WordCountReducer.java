/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isRedLink = true;
        HashSet<String> myValues = new HashSet<>();
        for (Text t : values) {
            if (t.toString().trim().equalsIgnoreCase("!")) {
                isRedLink = false;
            }
            myValues.add(t.toString());
        }
        if (!isRedLink) {
            for (String t : myValues) {
                context.write(key, new Text(t));
            }
        }
    }
}
