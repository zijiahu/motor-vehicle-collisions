import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.lang.*;
import java.util.*;
public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		
		//splits the input string only when there is even number of quotes prior, so ignores cases 
		//where there are commas in between quotes which is what the csv file wraps column inputs with 
		//"forbidden" characters like new line or commas around 
		String[] firstPass = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		
		//replace blank columns with NO INPUT
		for (int i = 0; i < firstPass.length; i++) {
			if(firstPass[i].equals("")) {
				firstPass[i] = "NO INPUT";
			}
		}
		
		//remove columns 
		String[] afterRemovals = new String[28];
		int tracker = 0;
		for(int i = 0; i < firstPass.length; i++) {
			if(i == 0 || i == 1 || i == 5) {
				continue;
			}
			else {
				afterRemovals[tracker] = firstPass[i];
				tracker++;
			}
		}
		
		//consolidate data 
			
		String[] consolidatedData = new String[3];
		tracker = 0;
		int consIterator = 0;
		Integer total = 0; 
		while(consIterator < afterRemovals.length) {
			if(consIterator == 0) {
				if(afterRemovals[consIterator] == null){
					consolidatedData[tracker] = "NO INPUT";
					tracker ++;
					consIterator = 3;
				}
				
				else if(afterRemovals[consIterator].equals("Roadway Name")){
					String newTitle = "Road Aggregate";
					consolidatedData[tracker] = newTitle;
					consIterator = 3; 
					tracker++;
				}
				else {
					String newStr = afterRemovals[0] + "$" + afterRemovals[1] + "$" + afterRemovals[2];
					consolidatedData[tracker] = newStr;
					consIterator = 3;
					tracker++;
				}
				
			}
			else if(consIterator >= 4) {
				if(afterRemovals[consIterator] == null){
					consolidatedData[tracker] = "NO INPUT";
					tracker ++;
					consIterator++;
				}
				else if(afterRemovals[consIterator].equals("12:00-1:00 AM")){
					String newTitle = "Traffic Volume";
					consolidatedData[tracker] = newTitle;
					consIterator = 28; //break out
					continue;
				}
				else if(afterRemovals[consIterator].equals("NO INPUT")){
					consIterator++; //
					continue; 
				}
				else{
					//strip quotes from ""1,443"" types
					String sanitize = afterRemovals[consIterator];
					int length = sanitize.length();
					if (sanitize.charAt(0) == '"') {
						sanitize = sanitize.substring(1, length-1).replace(",", "");
					}
					total = Integer.parseInt(sanitize);
					String newStr = Integer.toString(total);
					consolidatedData[tracker] = newStr;
					consIterator ++;
					//keep overwriting it...inefficeint but easy to code
				}
			}
			else {
				consolidatedData[tracker] = afterRemovals[consIterator];
				tracker++;
				consIterator++;
			}
		}
		for (int i = 0; i < consolidatedData.length; i++) {
			if(consolidatedData[i].equals("")) {
				consolidatedData[i] = "NO INPUT";
			}
		}
		
		context.write(new Text(""), new Text(consolidatedData[0] +"," + consolidatedData[1] +"," + consolidatedData[2]));
		
	}
}