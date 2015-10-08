/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double newPageRank = 0;
        /**
         * Pattern to distinguish our inserted numbers from numbers in titles
         * is: _!(numbers.numbers)
         */
        Pattern pt = Pattern.compile("(_!\\d+.\\d+)");
        String outGraph = "";
        for (Text t : values) {
            String s = t.toString();
            String[] ss = s.split(" ");
            /**
             * If value has more than 1 element, those are outLinks, keep them
             */
            if (ss.length > 1) {
                for (int i = 1; i < ss.length; i++) {
                    outGraph += " " + ss[i];
                }
            }
            /**
             * Read pageRanks and sum them up
             */
            Matcher mt = pt.matcher(s);
            if (mt.find()) {
                newPageRank += Double.parseDouble(mt.group(1).substring(2));
            }
        }
        outGraph = outGraph.trim();
        if (!outGraph.isEmpty()) {
            outGraph = " " + outGraph;
        }
//        context.write(key, new Text("_!" + new DecimalFormat("#0.0000000").format(newPageRank) + " " + outGraph));
        context.write(key, new Text("_!" + newPageRank + "" + outGraph));
    }
}
