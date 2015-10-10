/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.stream.XMLInputFactory;
import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;
import javax.xml.stream.XMLStreamReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.xml.stream.XMLStreamException;

public class RedLinkMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Possible single element nowiki tag makes xmlstreamparser to stop. Remove them first.
        String fixed = value.toString().replaceAll("<nowiki />|&lt;nowiki /&gt;", "");
        try {
            XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(new ByteArrayInputStream(fixed.getBytes()));
            String title = "";
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
                            textData += reader.getText();
                        }
                        break;
                }
            }
            reader.close();
            //At this point we have the title and text data ready.
            title = title.trim().replaceAll(" ", "_");
            /**
             * Find type 1 links e.g. [[some text]] and type 2 links [[a|b]]
             */
            ArrayList<String> myLinks = new ArrayList<>();
            try {
                myLinks = findLinks(textData);
            } catch (Exception e) {
                Logger.getLogger(RedLinkMapper.class.getName()).log(Level.SEVERE, e.getMessage(), e);
            }
            /**
             * For every title that exists, write the title and "!"
             */
            context.write(new Text(title), new Text("!"));

            for (int i = 0; i < myLinks.size(); i++) {
                //Write (link,title) pairs (inlinks) (multiple writes are ok)
                String temp = myLinks.get(i).replaceAll(" ", "_").split("\\|")[0];
                if (!title.equals(temp)) {
                    context.write(new Text(temp), new Text(title));
                }

            }
        } catch (XMLStreamException ex) {
            Logger.getLogger(RedLinkMapper.class.getName()).log(Level.SEVERE, ex.toString(), ex);
        }
    }
    /**
     *
     *@param data text data from XML parser that contains links with [[ ]] format
     *@return an ArrayList containing all the links (inner links are extracted as well).
     */
    public static ArrayList<String> findLinks(String data) {
        ArrayList<String> list = new ArrayList<>();
        Stack<Integer> s = new Stack<>();
        int i = 0;
        while (i < data.length() - 1) {
            if (data.charAt(i) == '[' && data.charAt(i + 1) == '[') {
                s.push(i + 2);
                i += 2;
                continue;
            }
            if (data.charAt(i) == ']' && data.charAt(i + 1) == ']' && !s.empty()) {
                list.add(data.substring(s.pop(), i));
                i += 2;
                continue;
            }
            i += 1;
        }
        return list;
    }
}
