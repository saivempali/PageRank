package pageRank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank {
	public static void main(String[] args) throws Exception {
		int numIter = 8;
		
		// JOB 1 : Counting number of pages
		new PageRank().pageCount(args);
		
		// JOB 2 : Page Rank Calculator
		new PageRank().pageRankCalculation(args, numIter);
		
		// JOB 3 : Sorting and displaying pages with ranks >= 5/N
		new PageRank().pageRankSorter(args, args[1] + "/temp/iter1.out");
		new PageRank().pageRankSorter(args, args[1] + "/temp/iter8.out");		
}

	
public String getNumNodes(Configuration job, String[] args){
		String line = "";
		  try{
              Path path=new Path(args[1] + "/num_nodes");
              FileSystem fs = FileSystem.get(path.toUri(),job);
              BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
              line=br.readLine();
		  }
		  catch(Exception e){
      }
		  
	return line;
	}

	
public void pageCount(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "pageCount");
		
		job.setJarByClass(PageCounterMapper.class);
		
		job.setMapperClass(PageCounterMapper.Map.class);
		job.setReducerClass(PageCounterReducer.Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp/num_nodes"));
		
		int exit = job.waitForCompletion(true) ? 0 : 1;
		

		//FileSystem fs = FileSystem.get(conf);
		//fs.rename(new Path(args[1] + "/temp/num_nodes/part-r-00000"), new Path(args[1] + "/num_nodes"));
		
		String inputFileName = args[1] + "/temp/num_nodes";
		String outputFileName = args[1] + "/num_nodes";
		
		final FileSystem fs1 = FileSystem.get(new Path(inputFileName).toUri(), conf);
		final FileSystem fs2 = FileSystem.get(new Path(outputFileName).toUri(), conf);
		FileUtil.copyMerge(fs1, new Path(inputFileName), fs2, new Path(outputFileName), true, conf, null);
	}

public void pageRankCalculation(String[] args, int numIter)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("numNodes", getNumNodes(conf, args));
		Job job;

		FileSystem fs;
		
		Path inpath = new Path(" ");
		Path outpath = new Path(" ");

		for (int i = 1; i <= numIter; i++) {
			
			if(i == 1){
				inpath = new Path(args[0]);
			}
			else{
				inpath = new Path(args[1] + "/temp/iter" + String.valueOf(i-1) + ".out");
			}
			outpath = new Path(args[1] + "/temp/iter" + String.valueOf(i) + ".out");
			
			fs = FileSystem.get(conf);
			job = Job.getInstance(conf, "pageRankCalculation");
			
			job.setJarByClass(PageRankMapper.class);
			
			job.setMapperClass(PageRankMapper.Map.class);
			job.setReducerClass(PageRankReducer.Reduce.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setMapOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			
			FileInputFormat.setInputPaths(job, inpath);
			FileOutputFormat.setOutputPath(job, outpath);
			
			int exit = job.waitForCompletion(true) ? 0 : 1;
			
		}
}
	
public void pageRankSorter(String[] args, String inputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
	
		
		Configuration conf = new Configuration();
		conf.set("numNodes", getNumNodes(conf, args));
	
		Job job = Job.getInstance(conf,"pageRankSorter");
		
		job.setJarByClass(SortMapper.class);
		
		job.setMapperClass(SortMapper.Map.class);
		job.setReducerClass(SortReducer.Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(" ");
		String inputFileName = "";
		String outputFileName = "";
		
		if(inputPath.contains("iter1")){
			outputPath = new Path(args[1] + "/temp/sortOutput/iter1.out");
			inputFileName = args[1] + "/temp/sortOutput/iter1.out";
			outputFileName = args[1] + "/iter1.out";
		}
		else{
			outputPath = new Path(args[1] + "/temp/sortOutput/iter8.out");
			inputFileName = args[1] + "/temp/sortOutput/iter8.out";
			outputFileName = args[1] + "/iter8.out";
		}
		
		
		FileInputFormat.setInputPaths(job,new Path(inputPath));
		FileOutputFormat.setOutputPath(job, outputPath);
		
		int exit = job.waitForCompletion(true) ? 0 : 1;
		
		//FileSystem fs = FileSystem.get(conf);
		//fs.rename(new Path(inputFileName), new Path(outputFileName));
		
		final FileSystem fs1 = FileSystem.get(new Path(inputFileName).toUri(), conf);
		final FileSystem fs2 = FileSystem.get(new Path(outputFileName).toUri(), conf);
		FileUtil.copyMerge(fs1, new Path(inputFileName), fs2, new Path(outputFileName), true, conf, null);
		
	}

}