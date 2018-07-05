package ass;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TvReducer extends Reducer <Text,IntWritable,Text,IntWritable>{
	private IntWritable result = new IntWritable();
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		// add the values and produce output for the key
		int sum=0;
		for (IntWritable v : values)
		{
			sum += v.get();
		}
		result.set(sum);
		context.write(key, result);
		
		
	}

}