package org.bigdata;

// Handles potential I/O errors.
import java.io.IOException;


import java.util.StringTokenizer;

//Hadoop Libraries:

// A Hadoop wrapper for an integer (int), used for output values.
import org.apache.hadoop.io.IntWritable;

// A Hadoop wrapper for a long integer (long), used for input keys.
import org.apache.hadoop.io.LongWritable;

//A Hadoop wrapper for a string (String), used for text input and output.
import org.apache.hadoop.io.Text;

// The base class for Mapper and Reducer in the old MapReduce API
import org.apache.hadoop.mapred.MapReduceBase;

//Defines the interface for the Mapper class.
import org.apache.hadoop.mapred.Mapper;

//Used to collect key-value pairs for output.
import org.apache.hadoop.mapred.OutputCollector;

//Reports progress, status, or custom counters in Hadoop.
import org.apache.hadoop.mapred.Reporter;

/*
  WC_Mapper → Name of the Mapper class (Word Count Mapper)
  extends MapReduceBase → Inherits from MapReduceBase, which provides default implementations for some methods.
  implements Mapper<LongWritable, Text, Text, IntWritable> → Implements the Mapper interface:
    - Input Key (LongWritable) → The byte offset of the line in the input file.
    - Input Value (Text) → The actual line of text.
    - Output Key (Text) → Each word in the input text.
    - Output Value (IntWritable) → Integer 1, representing a count for the word.
 */

public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable> {

    // Represents the constant value 1, which is assigned to each word occurrence.
    private final static IntWritable one = new IntWritable(1);

    //A Hadoop Text object used to store words before output.
    private Text word = new Text();


    public void map(LongWritable key, Text value,OutputCollector<Text,IntWritable> output,
                    Reporter reporter) throws IOException{
        String line = value.toString();
        StringTokenizer  tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()){
            word.set(tokenizer.nextToken());
            output.collect(word, one);
        }
    }
}
