import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

public class CrashData extends Configured implements Tool
{
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new CrashData(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception
    {
        // Tool implementation
        Configuration conf = getConf();

        // Format JSON file for map jobs
        Path rawPath = new Path("/user/swn226/project/crash_data.json");
        Path inputPath = new Path("/user/swn226/project/crash_data_2.json");
        Path outputPath = new Path("/user/swn226/project/crash_data");
        FileSystem hdfs = FileSystem.get(conf);
        InputStreamReader reader = new InputStreamReader(hdfs.open(rawPath));
        BufferedReader crashDataFile = new BufferedReader(reader);
        JSONParser parser = new JSONParser();
        JSONObject crashDataJSON = (JSONObject) parser.parse(crashDataFile);
        JSONArray crashData = (JSONArray) crashDataJSON.get("data");
        OutputStreamWriter writer = new OutputStreamWriter(hdfs.create(inputPath, true));
        for(Object line : crashData)
            writer.append(((JSONArray) line).toJSONString() + "\n");
        writer.close();

        // Create job
        Job job = Job.getInstance(conf, "Crash Data");
        job.setJarByClass(CrashData.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapperClass(CrashDataMapper.class);
        job.setReducerClass(CrashDataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
