import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class AggregateReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException{ 

	int collisions=0;
	int injuries=0;
	int deaths=0;
	String line = "";
	for (Text value : values) {
		line = value.toString();
		String[] columns = line.split(",");
		try{
		collisions+=Integer.valueOf(columns[0]);
	        injuries+=Integer.valueOf(columns[1]);
	        deaths+=Integer.valueOf(columns[2]);
		}
		catch(Exception e){
		collisions+=1;}
	 }
	String newKey = key.toString()+",";
	String output = ","+Integer.toString(collisions)+","+Integer.toString(injuries)+","+Integer.toString(deaths);
	context.write(new Text(newKey), new Text(output));
	}
}
