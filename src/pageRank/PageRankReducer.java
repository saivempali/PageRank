package pageRank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PageRankReducer {

	public static class Reduce extends Reducer<Text,Text,Text,Text> {

		private double d = 0.85;

		public void reduce(Text key, Iterable<Text> values,Context context
                )
			throws IOException, InterruptedException {
			
			double N = Double.parseDouble(context.getConfiguration().get("numNodes")); 
			double score = 0.00;
			StringBuilder sb = new StringBuilder();
			String[] links ;
			
			for (Text link:values) {
				
				links = link.toString().split("[ \t]");
				Double rank = Double.valueOf(links[1]);
				
				if(links[0].equals("$")) {
					if (links.length > 2) {
						for (int i = 2; i < links.length; i++) {
							sb.append(links[i] + " ");
						}
					}
				}
				else {
					Double outlinks = Double.valueOf(links[2]);	
					score = score + (rank/outlinks);
				} 
				
			}
			score = ((1 - d)/N) + (d * score);
			context.write(key, new Text(String.valueOf(score) + " " + sb.toString()));
		}
	}
}
