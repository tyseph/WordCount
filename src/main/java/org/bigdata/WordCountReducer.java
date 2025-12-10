package org.bigdata;

// Handles potential I/O errors.
import java.io.IOException;

//Used to iterate over values associated with a particular key.
import java.util.Iterator;

//Hadoop wrapper for an integer (int), used for input and output values.
import org.apache.hadoop.io.IntWritable;

//Hadoop wrapper for a string (String), used for input and output keys.
import org.apache.hadoop.io.Text;

//The base class for the old MapReduce API (deprecated in favor of org.apache.hadoop.mapreduce.Reducer).
import org.apache.hadoop.mapred.MapReduceBase;

//Used to collect and output key-value pairs.
import org.apache.hadoop.mapred.OutputCollector;

//Defines the interface for the Reducer class.
import org.apache.hadoop.mapred.Reducer;

//Reports progress, status, or custom counters in Hadoop.
import org.apache.hadoop.mapred.Reporter;

/*
WC_Reducer → Name of the Reducer class (Word Count Reducer).
extends MapReduceBase → Inherits from MapReduceBase, providing default method implementations.
implements Reducer<Text, IntWritable, Text, IntWritable> → Implements the Reducer interface:
    - Input Key (Text) → A word from the Mapper output.
    - Input Value (IntWritable) → A list of 1s corresponding to the occurrences of the word.
    - Output Key (Text) → The same word.
    - Output Value (IntWritable) → The total count of occurrences of the word.
 */

public class WordCountReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {

    /*
    reduce() → The core method of the Reducer that processes word-count pairs.
    Text key → A word from the Mapper output.
    Iterator<IntWritable> values → An iterator over all counts (1s) for the given word.
    OutputCollector<Text, IntWritable> output → Collects and outputs the final word count.
    Reporter reporter → Reports job progress and counters (not used here).
    throws IOException → Handles I/O exceptions.
     */
    public void reduce(Text key, Iterator<IntWritable> values,OutputCollector<Text,IntWritable> output,
                       Reporter reporter) throws IOException {
        int sum=0;

        //Summing Up Word Counts
        while (values.hasNext()) //Iterates over all occurrences of the word.
        {
            sum+=values.next().get(); // Extracts the integer value (1) from IntWritable and adds it to sum.
        }
        //Emitting the Final Word Count: Emits the word and its total count as the final output.
        output.collect(key,new IntWritable(sum));
    }
}
