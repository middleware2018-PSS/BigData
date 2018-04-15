package Query3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class Query3Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private Text distanceGroup= new Text();
    private IntWritable one = new IntWritable(1);
    private IntWritable zero = new IntWritable(0);

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String[] line = value.toString().split(",");
        if (line[0].equals("Year")) {

        }
        else if (line[18].equals("NA")) {

        }

        else if (line[15].equals("NA")) {

        }

        else if (line[14].equals("NA")) {

        }

        else if (((Integer.parseInt(line[15]) / 2) < Integer.parseInt(line[14])) && (Integer.parseInt(line[15]) > 0))  {
            int distanceGroupInt = (Integer.parseInt(line[18]) / 200) + 1;
            String distanceGroupString = Integer.toString(distanceGroupInt);
            distanceGroup.set(distanceGroupString);
            output.collect(distanceGroup, one);
        }

        else {
            int distanceGroupInt = (Integer.parseInt(line[18]) / 200) + 1;
            String distanceGroupString = Integer.toString(distanceGroupInt);
            distanceGroup.set(distanceGroupString);
            output.collect(distanceGroup, zero);
        }

    }
}