/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
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
        String test = value.toString();
        test = test.replaceAll("\t", " ");
        test = test.replaceFirst(" ", "\t");

        MathContext mc = new MathContext(19);
//        double basePageRank = 0;
        BigDecimal basePageRank = new BigDecimal(0, mc);
        boolean hasPageRank = false;
//        double pageRank = 0;
        BigDecimal pageRank = new BigDecimal(0, mc);
        /**
         * Pattern to distinguish our inserted numbers from numbers in titles
         * is: _!(numbers.numbers)
         */
        Pattern pt = Pattern.compile("(_!\\d+.\\S+)");
        Matcher mt = pt.matcher(test);
        if (mt.find()) {
//            pageRank = Double.parseDouble(mt.group(1).substring(2));
            pageRank = new BigDecimal(mt.group(1).substring(2), mc);
            hasPageRank = true;
        }
        /**
         * If it's the first iteration, distribute 1/N among outLinks
         */
        if (!hasPageRank) {
            try {
//                pageRank = 1d / (context.getConfiguration().getInt("N", 0));
                pageRank = new BigDecimal(1, mc);
                pageRank = pageRank.divide(new BigDecimal((context.getConfiguration().getInt("N", 0)), mc), mc);
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
         * Emit this node's (1-d)/N and it's adjacency outGraph if not empty
         */
        /**
         * d = 0.85
         */
//        basePageRank = (1 - 0.85) / (context.getConfiguration().getInt("N", 0));
        basePageRank = new BigDecimal(0.15, mc);
        basePageRank = basePageRank.divide(new BigDecimal((context.getConfiguration().getInt("N", 0)), mc), mc);
        String output = "";
        output += "_!" + basePageRank;
        if (split.length > 1) {
            String[] outlinks = split[1].split(" ");
            for (int i = hasPageRank ? 1 : 0; i < outlinks.length; i++) {
                output += " " + outlinks[i];
            }
        }
        output = output.trim();
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
             * Input has no outLinks, only has basePageRank, already taken care
             * of in previous emit, return
             */
            if (hasPageRank && outlinks.length == 1) {
                return;
            }
            /**
             * d = 0.85
             */
//            pageRank *= 0.85;
            /**
             * Divide pageRank over number of outLinks
             */
//            pageRank /= hasPageRank ? (outlinks.length - 1) : outlinks.length;
            if (hasPageRank) {
                pageRank = pageRank.divide(new BigDecimal((outlinks.length - 1), mc), mc);
            } else {
                pageRank = pageRank.divide(new BigDecimal(outlinks.length, mc), mc);
            }
            for (int i = hasPageRank ? 1 : 0; i < outlinks.length; i++) {
                context.write(new Text(outlinks[i]), new Text("_!" + pageRank));
            }
        }
    }
}
