package ass;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TvExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try
		{
			_main(args);
		}
		catch(Exception e)
		{
			System.out.println("Exception while running the job");
			e.printStackTrace();
		}
	}
	public static void _main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		
		System.out.println("Tv Example");
		
		if(args.length == 0)
		{
			System.out.println("Valid options are:");
			System.out.println("TvExample filter <input path> <output path>");
			System.out.println("TvExample CalcTotalUnitsPerCompany <input path> <output path>");
			System.out.println("TvExample CalcTotalUnitsForOnida <input path> <output path>");
			System.exit(-1);
		}
		
		TvExample tv = new TvExample();
		tv.tvRun(args);
		
		System.out.println("Tv Example Success");
	

}
	public TvExample()
	{
	    // constructor, nothing to be done here
		
	}
	private void tvRun(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		String option = args[0];
		Path input = new Path(args[1]);
		Path output = new Path(args[2]);
		// validate input output paths
	
		try
		{
			// this is only to run a mapper job to remove wrong entries from file
			if(option.equalsIgnoreCase("Filter"))
			{
				// init common stuff
				Job j = setupJob();
				// input and output path for the job
				FileInputFormat.setInputPaths(j, input);
				FileOutputFormat.setOutputPath(j, output);
				// set mapper class for this purpose
				j.setMapperClass(ass.TvMapper.class);
				// no reducers are needed for this
				j.setNumReduceTasks(0);
				executeJob(j);
			}
			// this is to do the calculations, no of products sold by each company 
			else if(option.equalsIgnoreCase("CalcTotalUnitsPerCompany"))
			{
				// init common stuff
				Job j = setupJob();
				// input and output path for the job
				FileInputFormat.setInputPaths(j, input);
				FileOutputFormat.setOutputPath(j,output);
				j.setMapperClass(ass.TvCalcMapper.class);
				j.setReducerClass(ass.TvReducer.class);
				j.setNumReduceTasks(1);
				executeJob(j);

			}// this is to do the calculations, no of products sold in each state by ONIDA 
			else if(option.equalsIgnoreCase("CalcTotalUnitsForOnida"))
			{
				// init common stuff
				Job j = setupJob();
				// input and output path for the job
				FileInputFormat.setInputPaths(j, input);
				FileOutputFormat.setOutputPath(j,output);
				j.setMapperClass(ass.TvCalcMapperOnida.class);
				j.setReducerClass(ass.TvReducer.class);
				j.setNumReduceTasks(1);
				executeJob(j);
				
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();

		}
		
		
	}
	private void executeJob(Job job) throws ClassNotFoundException, IOException, InterruptedException
	{
		if(job.waitForCompletion(true))
		{
			System.out.println("Job success");
		}
		else
		{
			System.out.println("Job Failed");
		}	
	}
	private Job setupJob() throws IOException
	{
		Job job = Job.getInstance();
		job.setJarByClass(TvExample.class);
		job.setJobName("TvExample");
		// input/output file formats for the job
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		// input/output file formats for the job
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);

		// type of K,V output of the job
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		
		return job;
		
	}
	

}



	
	
	
