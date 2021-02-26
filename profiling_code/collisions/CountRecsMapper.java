import java.io.IOException;
import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.LongWritable; import org.apache.hadoop.io.Text; import org.apache.hadoop.mapreduce.Mapper; 
public class CountRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable> { 
private static final int MISSING = 9999; 
@Override 
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
	String line = value.toString();
	context.write(new Text(""),new IntWritable(1));
} 
} 


