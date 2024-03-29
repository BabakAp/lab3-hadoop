/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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

public class PageRank extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PageRank(), args);
        System.exit(res);
    }

    @Override
    public int run(String args[]) {
        String tmp = "/tmp/" + new Date().getTime();
//        long timeStamp = new Date().getTime();
        try {
            /**
             * Job 1: Parse XML input and read title,links
             */
            Configuration conf = new Configuration();
            conf.set("xmlinput.start", "<page>");
            conf.set("xmlinput.end", "</page>");

            Job job = Job.getInstance(conf);
            job.setJarByClass(PageRank.class);

            // specify a mapper
            job.setMapperClass(RedLinkMapper.class);

            // specify a reducer
            job.setReducerClass(RedLinkReducer.class);

            // specify output types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // specify input and output DIRECTORIES
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(XmlInputFormat.class);

            FileOutputFormat.setOutputPath(job, new Path((args[1] + tmp + "/job1")));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.waitForCompletion(true);
        } catch (InterruptedException | ClassNotFoundException | IOException ex) {
            Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, ex.toString(), ex);
            System.err.println("Error during mapreduce job1.");
            return 2;
        }
        /**
         * Job 2: Adjacency outGraph
         */
        try {
            Configuration conf2 = new Configuration();

            Job job2 = Job.getInstance(conf2);
            job2.setJarByClass(PageRank.class);

            // specify a mapper
            job2.setMapperClass(AdjMapper.class);

            // specify a reducer
            job2.setReducerClass(AdjReducer.class);

            // specify output types
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            // specify input and output DIRECTORIES
            FileInputFormat.addInputPath(job2, new Path((args[1] + tmp + "/job1")));
            job2.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(job2, new Path((args[1] + tmp + "/job2")));
            job2.setOutputFormatClass(TextOutputFormat.class);

            job2.waitForCompletion(true);
        } catch (InterruptedException | ClassNotFoundException | IOException ex) {
            Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, ex.toString(), ex);
            System.err.println("Error during mapreduce job2.");
            return 2;
        }
        /**
         * Job 3: PageCount
         */
        try {
            Configuration conf3 = new Configuration();
            /**
             * Change output separator to "=" instead of default \t for this job
             */
            conf3.set("mapreduce.output.textoutputformat.separator", "=");

            Job job3 = Job.getInstance(conf3);
            job3.setJarByClass(PageRank.class);

            // specify a mapper
            job3.setMapperClass(PageCountMapper.class);

            // specify a reducer
            job3.setReducerClass(PageCountReducer.class);

            // specify output types
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(IntWritable.class);

            // specify input and output DIRECTORIES
            FileInputFormat.addInputPath(job3, new Path((args[1] + tmp + "/job2")));
            job3.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(job3, new Path((args[1] + tmp + "/job3")));
            job3.setOutputFormatClass(TextOutputFormat.class);

            job3.waitForCompletion(true);
        } catch (InterruptedException | ClassNotFoundException | IOException ex) {
            Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, ex.toString(), ex);
            System.err.println("Error during mapreduce job3.");
            return 2;
        }
        /**
         * Job 4: PageRank
         */
        for (int i = 1; i < 9; i++) {
            try {
                Configuration conf4 = new Configuration();
                /**
                 * Read number of nodes from the output of job 3 : pageCount
                 */
                Path path = new Path((args[1] + tmp + "/job3"));
                FileSystem fs = path.getFileSystem(conf4);
                RemoteIterator<LocatedFileStatus> ri = fs.listFiles(path, true);

                int n = 0;
                Pattern pt = Pattern.compile("(\\d+)");
                while (ri.hasNext()) {
                    LocatedFileStatus lfs = ri.next();
                    if (lfs.isFile() && n == 0) {
                        FSDataInputStream inputStream = fs.open(lfs.getPath());
                        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                        String s = null;
                        while ((s = br.readLine()) != null) {
                            Matcher mt = pt.matcher(s);
                            if (mt.find()) {
                                n = new Integer(mt.group(1));
                                break;
                            }
                        }
                    }
                }
                /**
                 * Done reading number of nodes, make it available to MapReduce
                 * job key: N
                 */
                conf4.setInt("N", n);

                Job job4 = Job.getInstance(conf4);
                job4.setJarByClass(PageRank.class);

                // specify a mapper
                job4.setMapperClass(PageRankMapper.class);

                // specify a reducer
                job4.setReducerClass(PageRankReducer.class);

                // specify output types
                job4.setOutputKeyClass(Text.class);
                job4.setOutputValueClass(Text.class);

                // specify input and output DIRECTORIES
                if (i == 1) {
                    FileInputFormat.addInputPath(job4, new Path((args[1] + tmp + "/job2")));
                } else {
                    FileInputFormat.addInputPath(job4, new Path((args[1] + tmp + "/job4/" + (i - 1))));
                }
                job4.setInputFormatClass(TextInputFormat.class);

                FileOutputFormat.setOutputPath(job4, new Path((args[1] + tmp + "/job4/" + i)));
                job4.setOutputFormatClass(TextOutputFormat.class);
                job4.waitForCompletion(true);
            } catch (InterruptedException | ClassNotFoundException | IOException ex) {
                Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, ex.toString(), ex);
                System.err.println("Error during mapreduce job4.");
                return 2;
            }
        }
        /**
         * Job 5: Sort iteration 1 and iteration 8
         */
        int returnCode = 0;
        for (int i = 0; i < 2; i++) {
            try {
                Configuration conf5 = new Configuration();

                /**
                 * Read number of nodes from the output of job 3 : pageCount
                 */
                Path path = new Path((args[1] + tmp + "/job3"));
                FileSystem fs = path.getFileSystem(conf5);
                RemoteIterator<LocatedFileStatus> ri = fs.listFiles(path, true);

                int n = 0;
                Pattern pt = Pattern.compile("(\\d+)");
                while (ri.hasNext()) {
                    LocatedFileStatus lfs = ri.next();
                    if (lfs.isFile() && n == 0) {
                        FSDataInputStream inputStream = fs.open(lfs.getPath());
                        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                        String s = null;
                        while ((s = br.readLine()) != null) {
                            Matcher mt = pt.matcher(s);
                            if (mt.find()) {
                                n = new Integer(mt.group(1));
                                break;
                            }
                        }
                    }
                }
                /**
                 * Done reading number of nodes, make it available to MapReduce
                 * job key: N
                 */
                conf5.setInt("N", n);

                Job job5 = Job.getInstance(conf5);
                /**
                 * one reducer only
                 */
                job5.setNumReduceTasks(1);
                job5.setSortComparatorClass(MyWritableComparator.class);
                job5.setJarByClass(PageRank.class);

                // specify a mapper
                job5.setMapperClass(SortMapper.class);
                job5.setMapOutputKeyClass(DoubleWritable.class);
                job5.setMapOutputValueClass(Text.class);

                // specify a reducer
                job5.setReducerClass(SortReducer.class);

                // specify output types
                job5.setOutputKeyClass(Text.class);
                job5.setOutputValueClass(DoubleWritable.class);

                // specify input and output DIRECTORIES
                int y = 7 * i + 1;
                FileInputFormat.addInputPath(job5, new Path((args[1] + tmp + "/job4/" + y)));
                job5.setInputFormatClass(TextInputFormat.class);

                FileOutputFormat.setOutputPath(job5, new Path((args[1] + tmp + "/job5/" + y)));
                job5.setOutputFormatClass(TextOutputFormat.class);

                returnCode = job5.waitForCompletion(true) ? 0 : 1;
            } catch (InterruptedException | ClassNotFoundException | IOException ex) {
                Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, ex.toString(), ex);
                System.err.println("Error during mapreduce job5.");
                return 2;
            }
        }
        /**
         * Copy necessary output files to args[1]        /**
         * Copy necessary output files to args[1]
         */

        /**
         * Rename and copy OutLinkGraph
         */
        try {
            Configuration conf = new Configuration();

            Path outLinkGraph = new Path((args[1] + tmp + "/job2/part-r-00000"));
            FileSystem outLinkGraphFS = outLinkGraph.getFileSystem(conf);

            Path output = new Path(args[1] + "/results/PageRank.outlink.out");
            FileSystem outputFS = output.getFileSystem(conf);
            org.apache.hadoop.fs.FileUtil.copy(outLinkGraphFS, outLinkGraph, outputFS, output, false, true, conf);
        } catch (IOException ex) {
            Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, ex.toString(), ex);
            System.err.println("Error while copying results.");
            return 2;
        }

        /**
         * Rename and copy total number of pages
         */
        try {
            Configuration conf = new Configuration();

            Path outLinkGraph = new Path((args[1] + tmp + "/job3/part-r-00000"));
            FileSystem outLinkGraphFS = outLinkGraph.getFileSystem(conf);

            Path output = new Path(args[1] + "/results/PageRank.n.out");
            FileSystem outputFS = output.getFileSystem(conf);
            org.apache.hadoop.fs.FileUtil.copy(outLinkGraphFS, outLinkGraph, outputFS, output, false, true, conf);
        } catch (IOException ex) {
            Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, ex.toString(), ex);
            System.err.println("Error while copying results.");
            return 2;
        }

        /**
         * Rename and copy iteration 1
         */
        try {
            Configuration conf = new Configuration();

            Path outLinkGraph = new Path((args[1] + tmp + "/job5/1/part-r-00000"));
            FileSystem outLinkGraphFS = outLinkGraph.getFileSystem(conf);

            Path output = new Path(args[1] + "/results/PageRank.iter1.out");
            FileSystem outputFS = output.getFileSystem(conf);
            org.apache.hadoop.fs.FileUtil.copy(outLinkGraphFS, outLinkGraph, outputFS, output, false, true, conf);
        } catch (IOException ex) {
            Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, ex.toString(), ex);
            System.err.println("Error while copying results.");
            return 2;
        }

        /**
         * Rename and copy iteration 8
         */
        try {
            Configuration conf = new Configuration();

            Path outLinkGraph = new Path((args[1] + tmp + "/job5/8/part-r-00000"));
            FileSystem outLinkGraphFS = outLinkGraph.getFileSystem(conf);

            Path output = new Path(args[1] + "/results/PageRank.iter8.out");
            FileSystem outputFS = output.getFileSystem(conf);
            org.apache.hadoop.fs.FileUtil.copy(outLinkGraphFS, outLinkGraph, outputFS, output, false, true, conf);
        } catch (IOException ex) {
            Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, ex.toString(), ex);
            System.err.println("Error while copying results.");
            return 2;
        }
        return returnCode;
    }
}
