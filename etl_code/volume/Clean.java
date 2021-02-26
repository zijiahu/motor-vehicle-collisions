//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Clean{
	public static void main(String[] args) throws Exception{
		//Configuration conf = new Configuration();
		//conf.set("mapreduce.textoutputformat.separator", ",");
		if(args.length != 2){
			System.err.println("Usage: Word Count <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(Clean.class);
		job.setJobName("Clean");
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(CleanMapper.class);
		job.setReducerClass(CleanReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
