package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SortMapper {


	public static class Map  extends Mapper<Object, Text, Text, Text>  {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] split = value.toString().split("[ \t]");
			context.write(new Text(String.valueOf(1.0 - Double.valueOf(split[1]))), new Text(split[0]));
		}
	}
}
