package pageRank;

import java.io.IOException;

import org.apache.commons.lang.NumberUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PageRankMapper {
	
	public static class Map extends Mapper<Object, Text, Text, Text> {
	
		private double rank = 1.0;
		private int outlinks = 0;
		private int outlinksStartPos = 0;

		@SuppressWarnings("deprecation")
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException  {
			
			String[] links = value.toString().split("[ \t]");
			double N = Double.parseDouble(context.getConfiguration().get("numNodes")); 

			if(links.length < 2) {
				rank = 1.0/N;
				outlinks = links.length - 1;
				context.write(new Text(links[0]), new Text("$ 0"));
			}
			else {
				
				if (NumberUtils.isNumber(links[1])) { 
					outlinksStartPos = 3;
					outlinks = links.length - outlinksStartPos + 1;

					rank = Double.valueOf(links[1]);
					
					StringBuilder sb = new StringBuilder();
					if (links.length >= outlinksStartPos) {
						for (int i = outlinksStartPos-1; i < links.length; i++) {
							sb.append(links[i] + " ");
						}
					}

					context.write(new Text(links[0]), new Text("$ " + String.valueOf(outlinks) + " " + sb.toString()));

					if (links.length >= outlinksStartPos) {
						for (int i = outlinksStartPos-1; i < links.length; i++) {
							context.write(new Text(links[i]),
									new Text(links[0] + " " + String.valueOf(rank) + " " + String.valueOf(outlinks)));
						}
					}
				}
				
				else { 
					rank = 1.0/N;
					outlinksStartPos = 2;
					outlinks = links.length - outlinksStartPos + 1;
					
					StringBuilder sb = new StringBuilder();
					if (links.length >= outlinksStartPos) {
						for (int i = outlinksStartPos-1; i < links.length; i++) {
							sb.append(links[i] + " ");
						}
					}
					
					context.write(new Text(links[0]), new Text("$ " + String.valueOf(outlinks) + " " + sb.toString()));

					if (links.length >= outlinksStartPos) {
						for (int i = outlinksStartPos-1; i < links.length; i++) {
							context.write(new Text(links[i]),
									new Text(links[0] + " " + String.valueOf(rank) + " " + String.valueOf(outlinks)));
						}
					}
				}
			}

		}

	}
}
