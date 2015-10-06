/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

/**
 * Credits:
 * http://log.malchiodi.com/2014/11/12/executing-jar-encoded-mapreduce-jobs-in-aws-either-through-web-interface-or-cli/
 */
import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.XMLInputFactory;
import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;
import javax.xml.stream.XMLStreamReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String document = value.toString();
        try {
            XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(new ByteArrayInputStream(document.getBytes()));
            String title = "";
            String links = "";
            String textData = "";
            String currentElement = "";
            while (reader.hasNext()) {
                int code = reader.next();
                switch (code) {
                    case START_ELEMENT:
                        currentElement = reader.getLocalName();
                        break;
                    case CHARACTERS:
                        if (currentElement.equalsIgnoreCase("title")) {
                            title += reader.getText();
                        } else if (currentElement.equalsIgnoreCase("text")) {
//                            TODO: String has a limit, will result in error: "constant string too long" if text is too big,
//                             we should do link extraction line by line
                            textData += reader.getText();
                        }
                        break;
                }
            }
            reader.close();

            title = title.replace(" ", "_");
            Pattern p = Pattern.compile("\\[\\[(.*?)\\]\\]");
            Matcher m = p.matcher(textData);
            while (m.find()) {
                links += " " + m.group(1);
            }

            context.write(title, links);
        } catch (Exception e) {
            //TODO: do something?
        }
    }
}
