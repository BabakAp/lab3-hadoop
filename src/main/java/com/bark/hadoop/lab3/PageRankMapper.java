/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * We have N, we don't need to read two files For first iteration: Read
         * adjacency graph, if no number found which is true for first
         * iteration, augment each line with 1/N, do stuff \n --- Reducer should
         * output e.g. A 0.25 B C (src) (PageRank) (outlink)... \n For next
         * iterations: Read adjacency graph, number will be found (due to output
         * format of reducer of last iteration), do stuff... \n
         */
//        String test = "A    _!55 2001 2020_World";
        String test = value.toString();
        boolean hasPageRank = false;
        double pageRank = 0;
        double oldPageRank = 0;
        /**
         * Pattern to distinguish our inserted numbers from numbers in titles
         * is: _!(numbers.numbers)
         */
        Pattern pt = Pattern.compile("(_!\\d+.\\d+)");
        Matcher mt = pt.matcher(test);
        if (mt.find()) {
            pageRank = Double.parseDouble(mt.group(1).substring(2));
            oldPageRank = pageRank;
            hasPageRank = true;
        }

        if (!hasPageRank) {
            try {
                pageRank = 1d / (context.getConfiguration().getInt("N", 0));
                /**
                 * d = 0.85
                 */
                oldPageRank = (1 - 0.85) / (context.getConfiguration().getInt("N", 0));
            } catch (ArithmeticException ae) {
                /**
                 * Catch division by zero (if 'N' was not set)
                 */
                Logger.getLogger(PageRankMapper.class.getName()).log(Level.SEVERE, ae.getMessage(), ae);
            }
        }
        /**
         * Split input line into key,value
         */
        String[] split = test.split("\t");
        /**
         * Emit this node's oldPageRank and it's adjacency outGraph if not empty
         */
        String output = "";
        output += "_!" + oldPageRank;
        if (split.length > 1) {
            output += " " + split[1];
        }
        context.write(new Text(split[0]), new Text(output));
        /**
         * Emit pageRank/|outLinks| to all outLinks if not empty: Split on \t to
         * get separate key(index 0) from values(index 1), Split values on space
         * to separate out links(ignore the first(index 0),the pageRank, unless
         * hasPageRank=false)
         */
        if (split.length > 1) {
            String[] outlinks = split[1].split(" ");
            /**
             * Input has no outLinks, only has oldPageRank, already taken care
             * of in previous emit, return
             */
            if (hasPageRank && outlinks.length == 1) {
                return;
            }
            /**
             * d = 0.85
             */
            pageRank *= 0.85;
            /**
             * Divide pageRank over number of outLinks
             */
            pageRank /= hasPageRank ? (outlinks.length - 1) : outlinks.length;

            for (int i = hasPageRank ? 1 : 0; i < outlinks.length; i++) {
//                System.out.println("");
                context.write(new Text(outlinks[i]), new Text("_!" + pageRank));
            }
        }
    }
}
