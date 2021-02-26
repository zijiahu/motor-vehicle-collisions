import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CleanMapper extends Mapper<LongWritable, Text, Text, Text>{
	public static String[] months = {"JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"};

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String[] line = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)",-1);
		// if(line.length != 36){
		// 	context.write(new Text("Invalid Data"), new IntWritable(1));
		// }

		/* Columns we need:
		4. ApplicationTypeShortDesc - String
		8. PermitSeriesShortDesc - String
		10. PermitTypeDesc - String
		22. PermitIssueDate - String
		23. IssuedWorkStartDate - String
		24. IssuedWorkEndDate - String
		25. BoroughName - String
		27. OnStreetName - String
		28. FromStreetName - String
		29. ToStreetName - String
		31. PermitPurposeComments - String
		32. PermitLocationComments - String
		35. CreatedOn - String
		36. ModifiedOn - String
		*/

		String applicationTypeShortDesc = line[3];
		String permitSeriesShortDesc = line[7];
		String permitTypeDesc = line[9];
		String permitIssueDate = line[21];
		String issuedWorkStartDate = line[22];
		String issuedWorkEndDate = line[23];
		String boroughName = line[24];
		String onStreetName = line[26];
		String fromStreetName = line[27];
		String toStreetName = line[28];
		String permitPurposeComments = line[30];
		String permitLocationComments = line[31];
		String createdOn = line[34];
		String modifiedOn = line[35];


		// modify PermitIssueDate
		if(!permitIssueDate.equals("") && 
			!permitIssueDate.equals("PermitIssueDate")){
			permitIssueDate = determineFunction(permitIssueDate);
		}

		// modify IssuedWorkStartDate
		if(!issuedWorkStartDate.equals("") && !issuedWorkStartDate.equals("IssuedWorkStartDate")){
			issuedWorkStartDate = determineFunction(issuedWorkStartDate);
		}

		// modify IssuedWorkEndDate
		if(!issuedWorkEndDate.equals("") && !issuedWorkEndDate.equals("IssuedWorkEndDate")){
			issuedWorkEndDate = determineFunction(issuedWorkEndDate);
		}

		// modify CreatedOn
		if(!createdOn.equals("") && !createdOn.equals("CreatedOn")){
			createdOn = determineFunction(createdOn);
		}

		// modify ModifiedOn
		if(!modifiedOn.equals("") && !modifiedOn.equals("ModifiedOn")){
			modifiedOn = determineFunction(modifiedOn);
		}


		context.write( new Text(" "), new Text(applicationTypeShortDesc + ","
			+ permitSeriesShortDesc + "," 
			+ permitTypeDesc + "," 
			+ permitIssueDate + "," 
			+ issuedWorkStartDate + "," 
			+ issuedWorkEndDate + "," 
			+ boroughName + "," 
			+ onStreetName + "," 
			+ fromStreetName + "," 
			+ toStreetName + "," 
			+ permitPurposeComments + "," 
			+ permitLocationComments + ","
			+ createdOn + ","
			+ modifiedOn));
	}

	public static boolean isStrDigit(String str_input){
		if(str_input.equals("") || str_input == null)
			return false;
		for(int i = 0; i < str_input.length(); i++){
			if( !Character.isDigit(str_input.charAt(i))){
				return false;
			}
		}
		return true;
	}


	public static int indexOf(Object[] strArray, Object element){
        	int index = Arrays.asList(strArray).indexOf(element);
        	return index;
	}


	
	// 11/13/2020 format
	public static String changeDateFormat1(String str_input){
		String newline = str_input.replace(" ", ":"); 
		String[] str = newline.split(":");
		boolean valid = false;
		if(str.length == 5 || str.length == 6){
			if(isStrDigit(str[1]) && isStrDigit(str[2]) &&isStrDigit(str[3])){
				valid = true;
			}
			if(valid){
				int hour = Integer.parseInt(str[1]);

				if(str[4].equals("PM")){
					hour+=12;
				}

				String str_output = str[0] + " " + hour + ":"+ str[2] + ":"+ str[3];

				return str_output;
			}
			else{
				return "INVALID";
			}
		}
		else{
			return "INVALID";
		}
	}



	public static String changeDateFormat2(String str_input){
		String newline = str_input.replace(" ", ":"); 
		String[] str = newline.split(":");
		if(str.length != 7 || str.length != 8) return "INVALID";
		boolean valid = false;
		if(isStrDigit(str[0]) && isStrDigit(str[2]) && isStrDigit(str[3]) && isStrDigit(str[4]) && isStrDigit(str[5])){
			valid = true;
		}
		if(valid){
			String mon = str[1].toUpperCase();
			int mon_int = indexOf(months, mon) + 1;
			if(mon_int == 0) return "INVALID";
			int hour = Integer.parseInt(str[3]);

			if(str[6].toUpperCase().equals("PM")){
				hour+=12;
			}

			String str_output = mon_int + "/" 
			+ str[2] + "/"
			+ str[0] + " "
			+ hour + ":"
			+ str[4] + ":"
			+ str[5];

			return str_output;
		}
		return "INVALID";
	}

	public static String determineFunction(String str_input){
		String newline = str_input.replace(" ", ":"); 
		String[] str = newline.split(":");
		if((str.length >=2) && isStrDigit(str[1])){
			return changeDateFormat1(str_input);
		}
		return changeDateFormat2(str_input);
	}


}