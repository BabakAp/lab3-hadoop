/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AdjReducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String line = "";
//        line += "_!";
        for (Text t : values) {
            String tString = t.toString();
            if (!tString.equals("!")) {
                line += "\t" + tString;
            }
        }
        line = line.trim();
        context.write(key, new Text(line));
    }
}
