import java.io.IOException;
import java.lang.Integer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONArray;

public class CrashDataMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
    {
        JSONParser parser = new JSONParser();
        try
        {
            JSONArray line = (JSONArray) parser.parse(value.toString());
            String zip = (String) line.get(11);
            int pedInjured = Integer.parseInt((String) line.get(20));
            int pedKilled = Integer.parseInt((String) line.get(21));
            int cyclistInjured = Integer.parseInt((String) line.get(22));
            int cyclistKilled = Integer.parseInt((String) line.get(23));
            int numInjured = pedInjured + pedKilled + cyclistInjured + cyclistKilled;
            if(zip != null && numInjured > 0)
                context.write(new Text(zip), new IntWritable(1));
        }
        catch(ParseException e)
        {
            e.printStackTrace();
        }
    }
}
