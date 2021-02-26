import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class AggregateReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException{ 

		int totalVolume = 0;
		int totalCount = 0;

		for (Text value : values) {
			String line = value.toString();
			try{
				totalVolume+=Integer.valueOf(line);
		        totalCount+=1;
			}
			catch(Exception e){
				totalVolume+=0;
			}
		}

		if (totalCount==0)totalCount=1;
		int avgVolume = Math.round(totalVolume/totalCount);
		String output = ","+Integer.toString(avgVolume);
		String newKey = key.toString()+",";
		context.write(new Text(newKey), new Text(output))
	}
}
