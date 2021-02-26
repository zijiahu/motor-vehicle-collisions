import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 
public class AggregateMapper extends Mapper<LongWritable, Text, Text, Text> { 
	private static final int MISSING = 9999; 
	@Override 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
		String line = value.toString();
		String[] columns = line.replaceAll("\t",",").split(",");
		
		String date = columns[0];
		String onstreet = columns[1].trim();
		String crossstreet = columns[2].trim();
		String borough = columns[3];
		String injured = columns[4];
		if (injured==""){injured="0";}
		String killed = columns[5];
		if(killed==""){killed="";}
		String id = columns[6];
		String time = columns[7];
		
		String streets = onstreet+"/"+crossstreet;
		String newcolumns = "1,"+injured+","+killed;
		if(streets.charAt(0)!='/' && streets.charAt(streets.length()-1)!='/'){
			context.write(new Text(streets),new Text(newcolumns));
		}	
	} 
} 


