import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProjectReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException{ 

	int count = 0;
	String line = "";
	for (Text value : values) {
		line = value.toString();
		count++;
	}

	String newKey = key.toString()+",";
	String output = ","+Integer.toString(count);
	context.write(new Text(newKey), new Text(output));
	}
}
