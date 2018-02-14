package recommendsFriend.RecommendsFriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;

public class My_Job {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf, "RecommendsFriends");
    job.setJarByClass(My_Job.class);
    
    job.setMapperClass(My_Mapper.class);
   // job.setCombinerClass(My_Combiner.class);
    job.setReducerClass(My_Reducer.class);
  
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	//Wait for the job to complete and print if the job was successful or not
	int returnValue = job.waitForCompletion(true) ? 0:1;
	
	if(job.isSuccessful()) {
		System.out.println("Job was successful");
	} else if(!job.isSuccessful()) {
		System.out.println("Job was not successful");			
	}
	
	return;
    
    
  }
}
