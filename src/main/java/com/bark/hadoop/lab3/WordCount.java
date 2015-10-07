/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
        long timeStamp = new Date().getTime();
        try {
            /**
             * Job 1: Parse XML input and read title,links
             */
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
            job.setOutputValueClass(Text.class);

            // specify input and output DIRECTORIES
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(XmlInputFormat.class);

            FileOutputFormat.setOutputPath(job, new Path((args[1] + "/" + timeStamp + "/job1")));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.waitForCompletion(true);
        } catch (InterruptedException | ClassNotFoundException | IOException ex) {
            Logger.getLogger(WordCount.class.getName()).log(Level.SEVERE, ex.toString(), ex);
            System.err.println("Error during mapreduce job1.");
            return 2;
        }
        /**
         * Job 2: Adjacency outGraph
         */
        try {
            Configuration conf2 = new Configuration();

            Job job2 = Job.getInstance(conf2);
            job2.setJarByClass(WordCount.class);

            // specify a mapper
            job2.setMapperClass(AdjMapper.class);

            // specify a reducer
            job2.setReducerClass(AdjReducer.class);

            // specify output types
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            // specify input and output DIRECTORIES
            FileInputFormat.addInputPath(job2, new Path((args[1] + "/" + timeStamp + "/job1")));
            job2.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(job2, new Path((args[1] + "/" + timeStamp + "/job2")));
            job2.setOutputFormatClass(TextOutputFormat.class);

            job2.waitForCompletion(true);
        } catch (InterruptedException | ClassNotFoundException | IOException ex) {
            Logger.getLogger(WordCount.class.getName()).log(Level.SEVERE, ex.toString(), ex);
            System.err.println("Error during mapreduce job2.");
            return 2;
        }
        /**
         * Job 3: PageCount
         */
        try {
            Configuration conf3 = new Configuration();

            Job job3 = Job.getInstance(conf3);
            job3.setJarByClass(WordCount.class);

            // specify a mapper
            job3.setMapperClass(PageCountMapper.class);

            // specify a reducer
            job3.setReducerClass(PageCountReducer.class);

            // specify output types
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(IntWritable.class);

            // specify input and output DIRECTORIES
            FileInputFormat.addInputPath(job3, new Path((args[1] + "/" + timeStamp + "/job2")));
            job3.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(job3, new Path((args[1] + "/" + timeStamp + "/job3")));
            job3.setOutputFormatClass(TextOutputFormat.class);

            job3.waitForCompletion(true);
        } catch (InterruptedException | ClassNotFoundException | IOException ex) {
            Logger.getLogger(WordCount.class.getName()).log(Level.SEVERE, ex.toString(), ex);
            System.err.println("Error during mapreduce job3.");
            return 2;
        }
        /**
         * Job 4: PageRank
         */
        try {
            Configuration conf4 = new Configuration();
            File folder = new File((args[1] + "/" + timeStamp + "/job3"));
            File[] listOfFiles = folder.listFiles();
            int n = 0;
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile() && n == 0) {
                    BufferedReader br = new BufferedReader(new FileReader(listOfFiles[i]));
                    String s = "";
                    while ((s = br.readLine()) != null) {
                        Pattern pt = Pattern.compile("(\\d+)");
                        Matcher mt = pt.matcher(s);
                        if (mt.find()) {
                            n = new Integer(mt.group(1));
                            break;
                        }
                    }
                }
            }
            conf4.setInt("N", n);

            Job job4 = Job.getInstance(conf4);
            job4.setJarByClass(WordCount.class);

            // specify a mapper
            job4.setMapperClass(PageRankMapper.class);

            // specify a reducer
            job4.setReducerClass(PageRankReducer.class);

            // specify output types
            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(Text.class);

            // specify input and output DIRECTORIES
            FileInputFormat.addInputPath(job4, new Path((args[1] + "/" + timeStamp + "/job3")));
            job4.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(job4, new Path((args[1] + "/" + timeStamp + "/job4")));
            job4.setOutputFormatClass(TextOutputFormat.class);

            return (job4.waitForCompletion(true) ? 0 : 1);
        } catch (InterruptedException | ClassNotFoundException | IOException ex) {
            Logger.getLogger(WordCount.class.getName()).log(Level.SEVERE, ex.toString(), ex);
            System.err.println("Error during mapreduce job4.");
            return 2;
        }
    }
}
