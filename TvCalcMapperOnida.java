package ass;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TvCalcMapperOnida extends Mapper<LongWritable,Text,Text,IntWritable> {
	private static final IntWritable one = new IntWritable(1);
	private Text word  = new Text();
	
	@Override 
	public void map(LongWritable key, Text val,Context context ) throws IOException, InterruptedException
	{
		
		String value = val.toString();
		String result[] = value.split("\\|");
		
		if(!result[0].equalsIgnoreCase("Onida") || result[1].equalsIgnoreCase("NA"))
		{
			// we are not interested in any company other than Onida which equals invalid value as NA
			// also if the product is NA
			return; 
		}
		
		
		// we are here which means we have a valid record for for Onida. Output state as the key
		// state is the 4th column in the row
		
		context.write(new Text(result[3]),one);
		
	}

}


