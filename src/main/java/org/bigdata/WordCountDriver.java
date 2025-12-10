package org.bigdata;

import java.io.IOException;

//Represents the path of the input/output files in the Hadoop Distributed File System (HDFS).
import org.apache.hadoop.fs.Path;

//A wrapper class for an integer (int) to be used in the MapReduce job.
import org.apache.hadoop.io.IntWritable;

//A wrapper class for a string (String) used for keys and values in the MapReduce job.
import org.apache.hadoop.io.Text;

//Defines the input format for reading input files.
import org.apache.hadoop.mapred.FileInputFormat;

//Defines the output format for writing output files.
import org.apache.hadoop.mapred.FileOutputFormat;

//Submits the job to the Hadoop cluster and manages its execution.
import org.apache.hadoop.mapred.JobClient;

//The job configuration, which holds the configuration settings for the MapReduce job.
import org.apache.hadoop.mapred.JobConf;

//Defines the input format for text files (default format for reading text).
import org.apache.hadoop.mapred.TextInputFormat;

//Defines the output format for writing text files (default format for writing text).
import org.apache.hadoop.mapred.TextOutputFormat;
public class WordCountDriver {
    public static void main(String[] args) throws IOException{

        //Job Configuration: Creates a new JobConf object for configuring the Hadoop job.
        //It uses the WC_Runner.class to identify the class to run for the job.
        JobConf conf = new JobConf(WordCountDriver.class);

        //Sets the name of the job to "WordCount" (this name is displayed in job logs).
        conf.setJobName("WordCount");

        //Setting Output Key and Value Classes
        //Specifies that the key output type will be Text (used for words).
        conf.setOutputKeyClass(Text.class);

        //Specifies that the value output type will be IntWritable (used for the count of each word).
        conf.setOutputValueClass(IntWritable.class);

        //Setting Mapper, Combiner, and Reducer Classes:
        //Specifies the Mapper class to use, which is WC_Mapper. This class processes the input and emits intermediate key-value pairs.
        conf.setMapperClass(WordCountMapper.class);

        //Specifies the Combiner class. A combiner performs partial aggregation (local reduction) on the output of the Mapper before sending it to the Reducer. Here, the WC_Reducer class is used.
        //The combiner is optional, but it is often used to optimize performance by reducing the amount of data transferred between the Mapper and Reducer.
        conf.setCombinerClass(WordCountReducer.class);

        //Specifies the Reducer class to use, which is WC_Reducer. This class takes the intermediate results and reduces them to the final output.
        conf.setReducerClass(WordCountReducer.class);

        // Setting Input and Output Formats:
        // Specifies the format for reading input files. TextInputFormat is the default input format for text files in Hadoop,
        // where each line is a key-value pair (LineNumber, LineText).
        conf.setInputFormat(TextInputFormat.class);

        // Specifies the format for writing output files. TextOutputFormat is the default output format,
        // where the output is written as text files with key-value pairs.
        conf.setOutputFormat(TextOutputFormat.class);

        //Setting Input and Output Paths:
        //Specifies the input directory path for the job.
        // The input path is provided as the first argument in the command-line input (args[0]).
        FileInputFormat.setInputPaths(conf,new Path(args[0]));

        //Specifies the output directory path for the job.
        // The output path is provided as the second argument in the command-line input (args[1]).
        //The output path must not already exist, as Hadoop will create the output directory during execution.
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));

        //Running the Job:
        //Submits the job configuration (conf) to the Hadoop cluster and runs the job.
        // The job will process the input files, execute the Mapper and Reducer,
        // and write the output to the specified output directory.
        JobClient.runJob(conf);
    }
}
