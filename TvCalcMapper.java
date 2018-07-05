package ass;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TvCalcMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	private static final IntWritable one = new IntWritable(1);
	private Text word  = new Text();
	
	@Override 
	public void map(LongWritable key, Text val,Context context ) throws IOException, InterruptedException
	{
		
		String value = val.toString();
		String result[] = value.split("\\|");
		
		if(result[0].equalsIgnoreCase("NA") || 
				result[1].equalsIgnoreCase("NA") )
		{
			return;
		}
		
		// we are here which means we have a valid record 
		// first element in result is the company name which is the key
		
		context.write(new Text(result[0]),one);
		
	}

}


