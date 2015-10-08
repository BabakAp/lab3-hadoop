/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        double pageRank = 0;
        Pattern pt = Pattern.compile("(_!\\d+.\\d+)");
        Matcher mt = pt.matcher(value.toString());
        if (mt.find()) {
            pageRank = Double.parseDouble(mt.group(1).substring(2));
        }
        context.write(new DoubleWritable(pageRank), new Text(value.toString().split("\t")[0]));
    }
}
