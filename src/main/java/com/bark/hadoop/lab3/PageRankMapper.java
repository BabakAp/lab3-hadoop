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

import org.apache.hadoop.io.IntWritable;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new Text("N"), new IntWritable(1));
        /**
         * We have N, we don't need to read two files
         * For first iteration: Read adjacency graph, if no number found which is true for first iteration, augment each line with 1/N, do stuff \n
         *  --- Reducer should output e.g.  A 0.25 B C   (src) (PageRank) (outlink)... \n
         * For next iterations: Read adjacency graph, number will be found (due to output format of reducer of last iteration), do stuff... \n
         */
        System.out.println(context.getConfiguration().getInt("N",0));
//            context.getConfiguration().getInt(null, defaultValue)
    }
}
