package pageRank;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PageCounterReducer extends Configured {

	public static class Reduce extends Reducer<Text,IntWritable,Text,Text> {
		public void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
			int totalCount = 0;
			for (IntWritable val:values) {
				totalCount  = totalCount + val.get();
			}
			context.write(null,new Text(String.valueOf(totalCount)));
		}
	}

}
