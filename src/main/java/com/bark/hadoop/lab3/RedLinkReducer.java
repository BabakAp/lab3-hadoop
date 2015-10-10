/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RedLinkReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isRedLink = true;
        //hashset to remove duplicates
        HashSet<String> myValues = new HashSet<>();
        for (Text t : values) {
            //if there exists a pair for page A with value ! ( ie. (A,!) ) page A exists and therefor the link is not a redlink
            if (t.toString().trim().equalsIgnoreCase("!")) {
                isRedLink = false;  
            }
            myValues.add(t.toString().trim());
        }
        //if the link is not identified as redlink, write it to ouput. else ignore.
        if (!isRedLink) {
            for (String t : myValues) {
                context.write(key, new Text(t));
            }
        }
    }
}
