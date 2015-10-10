/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    @Override
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //reducer is given (pagerank,page) pairs. Internally sorts it (@see MyWritableComparator). 
        //prints out (page,pagerank) pairs as output.
        for(Text t: values) {
            context.write(t, key);
        }
    }
}
