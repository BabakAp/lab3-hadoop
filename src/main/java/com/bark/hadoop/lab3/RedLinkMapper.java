/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bark.hadoop.lab3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.stream.XMLInputFactory;
import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;
import javax.xml.stream.XMLStreamReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import javax.xml.stream.XMLStreamException;

public class RedLinkMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        if (value.toString().contains("&lt;nowiki /&gt;")) {
//            System.out.println("nowiki detected");
//        }
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
//                            TODO: String has a limit, will result in error: "constant string too long" if text is too big,
//                             we should do link extraction line by line
//                            UPDATE: maybe not! Apparently the limit is only for 'constant' strings, not strings constructed during runtime :|
                            textData += reader.getText();
                        }
                        break;
                }
            }
            reader.close();

            title = title.trim().replaceAll(" ", "_");
//            title = title.toLowerCase();
            /**
             * Find type 1 links e.g. [[some text]] and type 2 links [[a|b]]
             */
            ArrayList<String> myLinks = findLinks(textData);
//            for (String s : my) {
//                links += " " + s.trim().replaceAll(" ", "_").split("\\|")[0];
//            }
//            Pattern p = Pattern.compile("\\[\\[([^\\[\\[\\]\\]]*?)\\]\\]");
//            Matcher m = p.matcher(textData);
//            while (m.find()) {
//                String newlink = (m.group(1)).trim().replaceAll(" ", "_").split("\\|")[0];
//                //TODO: "it should not contain a link which points to the page itself"
//                if (!title.equals(newlink)) {
//                    links += " " + newlink;
//                }
//            }
       //     links = links.trim();
            /**
             * For every title that exists, write the title and "!"
             */
            context.write(new Text(title), new Text("!"));
//            links = links.replaceAll(title, "");
  //          links = links.trim();
   //         String[] myLinks = links.split(" ");
            for (int i = 0; i < myLinks.size(); i++)  {
//                Write reverse? (link,title) pairs (multiple writes are ok)
                context.write(new Text(myLinks.get(i).replaceAll(" ", "_").split("\\|")[0]), new Text(title));
            }
        } catch (XMLStreamException ex) {
            Logger.getLogger(RedLinkMapper.class.getName()).log(Level.SEVERE, ex.toString(), ex);
        }
    }

    /**
     * @param textData
     * @param args the command line arguments
     * @return
     */
    public static ArrayList<String> findLinks(String textData) {
        ArrayList<String> list = new ArrayList<>();
        int state = 0;
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < textData.length();) {
            if (state == 0) {
                if (textData.charAt(i) == '[') {
                    state = 1;
                }
                i++;
            } else if (state == 1) {
                if (textData.charAt(i) == '[') {
                    state = 2;
                }
                i++;
            } else if (state == 2) {
                if (textData.charAt(i) == '[') {
                    state = 3;
                }
                if (textData.charAt(i) == ']') {
                    state = 5;
                }
                buffer.append(textData.charAt(i));
                i++;
            } else if (state == 3) {
                if (textData.charAt(i) == '[') {
                    state = 2;
                    buffer.delete(0, buffer.length());
                    i++;
                } else {
                    state = 2;
                }
            } else if (state == 5) {
                if (textData.charAt(i) == ']') {
                    buffer.delete(buffer.length() - 1, buffer.length());
                    state = 0;
                    list.add(buffer.toString());
                    buffer.delete(0, buffer.length());
                    i++;
                } else {
                    state = 2;
                }
            }
        }
        return list;
    }
}
