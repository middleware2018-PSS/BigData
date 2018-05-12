package Query6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;

import java.io.IOException;

public class Query6Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private Text yearOrigin = new Text();
    private IntWritable taxiIn = new IntWritable(1);

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String[] line = value.toString().split(",");
        if (line[0].equals("Year") || line[0].equals("NA") || line[16].equals("NA") || line[19].equals("NA")) {

        }
        else{
            yearOrigin.set("Year: " + line[0] + " Origin: " + line[16]);
            taxiIn.set(Integer.parseInt(line[19]));
            output.collect(yearOrigin, taxiIn);
        }
    }
}