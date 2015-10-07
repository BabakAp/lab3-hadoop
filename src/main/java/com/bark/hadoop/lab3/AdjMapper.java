/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author babak alipour (babak.alipour@gmail.com)
 */
public class AdjMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * Split up the line into (key,value) pair - separated by tab
         */
        String[] document = value.toString().split("\t");
        /**
         * Nice names for better readability
         */
        String myKey = document[0];
        String myValue = document[1];
        /**
         * If (A,!), write it, else it's (B,A)-inGraph, write (A,B)-outGraph
         */
        if (myValue.equals("!")) {
            context.write(new Text(myKey), new Text(myValue));
        } else {
            context.write(new Text(myValue), new Text(myKey));
        }
    }
}
