import java.io.IOException;
import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class CleanReducer
extends Reducer<Text, IntWritable, Text, Text>{
public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException{ 
for (Text value : values) {
context.write(key,value); }}}
