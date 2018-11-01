package Query3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;

import java.io.IOException;
//sarebbe Int, IntWritbale>
public class Query3Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private Text distanceGroup= new Text();
    private IntWritable one = new IntWritable(1);
    private IntWritable zero = new IntWritable(0);

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        String[] line = value.toString().split(",");

        if not (line[0].equals("Year") || line[18].equals("NA") || line[15].equals("NA") || line[14].equals("NA")) {

            int distanceGroupInt = (Integer.parseInt(line[18]) / 200) + 1;

            String distanceGroupString = Integer.toString(distanceGroupInt);//tolgo

            distanceGroup.set(distanceGroupString); //.set(distanceGroupInt)

            if (((Integer.parseInt(line[15]) / 2) >= Integer.parseInt(line[14])) && (Integer.parseInt(line[15]) > 0)) {

                output.collect(distanceGroup, one);

            }

            else {

                output.collect(distanceGroup, zero);

            }

        }

    }

}