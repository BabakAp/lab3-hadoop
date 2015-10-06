/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
	}

    @Override
    public int run(String args[]) {
        try {
            Configuration conf = new Configuration();
            conf.set("xmlinput.start", "<page>");            
            conf.set("xmlinput.end", "</page>");

            Job job = Job.getInstance(conf);
            job.setJarByClass(WordCount.class);

            // specify a mapper
            job.setMapperClass(WordCountMapper.class);

            // specify a reducer
            job.setReducerClass(WordCountReducer.class);

            // specify output types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // specify input and output DIRECTORIES
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(XmlInputFormat.class);

            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setOutputFormatClass(TextOutputFormat.class);

            return(job.waitForCompletion(true) ? 0 : 1);
        } catch (InterruptedException|ClassNotFoundException|IOException e) {
            System.err.println("Error during mapreduce job.");
            e.printStackTrace();
            return 2;
        }
    }
}