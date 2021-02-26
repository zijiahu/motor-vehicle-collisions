import java.io.IOException;
import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.LongWritable; import org.apache.hadoop.io.Text; import org.apache.hadoop.mapreduce.Mapper; 
public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> { 
private static final int MISSING = 9999; 
@Override 
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
	String line = value.toString();
	String COMMA_INSIDE = ",(?=[^()]*\\))";
	String line2 = line.replaceAll(COMMA_INSIDE,"/").replaceAll("\t"," ");
	String[] columns = line2.split(",");
	if(columns.length>=23){
		String date = columns[0].trim();
        String borough = columns[2];
        String onstreet = columns[7].trim().toLowerCase();
        String crossstreet = columns[8].trim().toLowerCase();
        String injured = columns[10];
        String killed = columns[11];
        String id = columns[23];
        String time = columns[1].trim();
        String newcolumns = onstreet+","+crossstreet+","+borough+","+injured+","+killed+","+id+","+time;
        context.write(new Text(date),new Text(newcolumns));
}

} 
} 


