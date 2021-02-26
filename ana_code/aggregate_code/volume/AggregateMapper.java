import java.io.IOException;
import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.LongWritable; import org.apache.hadoop.io.Text; import org.apache.hadoop.mapreduce.Mapper; 
public class AggregateMapper extends Mapper<LongWritable, Text, Text, Text> { 
private static final int MISSING = 9999; 
@Override 
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
	String line = value.toString();
	String[] columns = line.trim().split(",");
	String street = columns[0];
	String date = columns[1];
	String volume = columns[2];


	String[] streets = street.split("\\$");

	String onStreet = "";


	if (streets.length>=2){
		onStreet = streets[0].trim().toLowerCase();
		String fromStreet = streets[1].trim().toLowerCase();

		String street_name1 = onStreet + "/" + fromStreet;
		street_name1 = street_name1.replace('\t',' ').replaceAll("    "," ").replaceAll("   "," ").replaceAll("  "," ");
		if(street_name1.charAt(0)!='/' && street_name1.charAt(street_name1.length()-1)!='/'){
			context.write(new Text(street_name1),new Text(volume));
		}


	}

	if (streets.length>=3){
		onStreet = streets[0].trim().toLowerCase();
		String toStreet = streets[2].trim().toLowerCase();

		String street_name2 = onStreet + "/" + toStreet;
		street_name2 = street_name2.replace('\t',' ').replaceAll("    "," ").replaceAll("   "," ").replaceAll("  "," ");
		if(street_name2.charAt(0)!='/' && street_name2.charAt(street_name2.length()-1)!='/'){
			context.write(new Text(street_name2),new Text(volume));
		}


	}

	

} 
} 


