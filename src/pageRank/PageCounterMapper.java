package pageRank;


import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PageCounterMapper extends Configured{
	
	public static class Map extends Mapper<Object, Text, Text, IntWritable>{

		public void map(Object key, Text value, Context context)
		        throws IOException, InterruptedException {	
			
			IntWritable one = new IntWritable(1);
			context.write(new Text("count"),one );
		}
	}

}
