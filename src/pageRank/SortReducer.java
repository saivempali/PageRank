package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SortReducer {

	public static class Reduce extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			double  N = Double.parseDouble(context.getConfiguration().get("numNodes"));
			Text value = new Text();
			for (Text page :values) {
				value.set(String.valueOf(1.0- Double.valueOf(key.toString())));
				if (Double.valueOf(value.toString()) >= 5/N) { 
					context.write(page,value);
				}
			}
		}
	}
}
