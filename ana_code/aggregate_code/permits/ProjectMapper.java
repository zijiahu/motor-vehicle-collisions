import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ProjectMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String[] line = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)",-1);

		/* Columns we need:
		0. ApplicationTypeShortDesc - String
		1. PermitSeriesShortDesc - String
		2. PermitTypeDesc - String
		3. PermitIssueDate - String
		4. IssuedWorkStartDate - String
		5. IssuedWorkEndDate - String
		6. BoroughName - String
		7. OnStreetName - String
		8. FromStreetName - String
		9. ToStreetName - String
		10. PermitPurposeComments - String
		11. PermitLocationComments - String
		12. CreatedOn - String
		13. ModifiedOn - String
		*/

		String applicationTypeShortDesc = line[0];
		if(applicationTypeShortDesc.contains("ApplicationTypeShortDesc")) return;
		String permitSeriesShortDesc = line[1];
		String permitTypeDesc = line[2];
		String permitIssueDate = line[3];
		String issuedWorkStartDate = line[4];
		String issuedWorkEndDate = line[5];
		String boroughName = line[6];
		String onStreetName = line[7].trim().toLowerCase();
		String fromStreetName = line[8].trim().toLowerCase();
		String toStreetName = line[9].trim().toLowerCase();
		String permitPurposeComments = line[10];
		String permitLocationComments = line[11];
		String createdOn = line[12];
		String modifiedOn = line[13];

		if (onStreetName.length()==0||fromStreetName.length()==0||toStreetName.length()==0)return;

		String street_name1 = onStreetName + "/" + fromStreetName;
		street_name1 = street_name1.replace('\t',' ').replace("    "," ").replace("   "," ").replace("  "," ");
		String street_name2 = onStreetName + "/" + toStreetName;
		street_name2 = street_name2.replace('\t',' ').replace("    "," ").replace("   "," ").replace("  "," ");

		context.write( new Text(street_name1), new Text(boroughName + "," + applicationTypeShortDesc));
		context.write( new Text(street_name2), new Text(boroughName + "," + applicationTypeShortDesc));
	}
}
