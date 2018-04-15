package Query2;

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

public class Query2Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private Text yearAndWeekYear = new Text();
    private IntWritable one = new IntWritable(1);
    private IntWritable zero = new IntWritable(0);

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String[] line = value.toString().split(",");
        if (line[0].equals("Year")) {

        }
        else if (line[25].equals("NA")) {
            Calendar calendar = new GregorianCalendar(Integer.parseInt(line[0]), Integer.parseInt(line[1]) - 1, Integer.parseInt(line[2]));
            String mapKey = "Year: " + line[0] + " Week of Year: " + calendar.get(Calendar.WEEK_OF_YEAR);
            yearAndWeekYear.set(mapKey);
            output.collect(yearAndWeekYear, zero);

        }
        else if (line[25].equals("0")) {
            Calendar calendar = new GregorianCalendar(Integer.parseInt(line[0]), Integer.parseInt(line[1]) - 1, Integer.parseInt(line[2]));
            String mapKey = "Year: " + line[0] + " Week of Year: " + calendar.get(Calendar.WEEK_OF_YEAR);
            yearAndWeekYear.set(mapKey);
            output.collect(yearAndWeekYear, zero);

        }

        else {
            Calendar calendar = new GregorianCalendar(Integer.parseInt(line[0]), Integer.parseInt(line[1]) - 1, Integer.parseInt(line[2]));
            String mapKey = "Year: " + line[0] + " Week of Year: " + calendar.get(Calendar.WEEK_OF_YEAR);
            yearAndWeekYear.set(mapKey);
            output.collect(yearAndWeekYear, one);
        }

    }
}