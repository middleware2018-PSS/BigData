package Query1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;

import java.io.IOException;

public class Query1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private Text date = new Text();
    private IntWritable isCancelled = new IntWritable();

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        String[] line = value.toString().split(",");

        if not (line[0].equals("Year") or line[0].equals("NA") or line[1].equals("NA") or line[2].equals("NA")) {
            String dateValue = "Year: " + line[0] + " Month: " + line[1] + " DayofMonth: " + line[2];
            date.set(dateValue);
            isCancelled.set(Integer.parseInt(line[21]));
            output.collect(date, isCancelled);
        }

    }
}