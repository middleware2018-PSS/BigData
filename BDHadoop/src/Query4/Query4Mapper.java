package Query4;

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

public class Query4Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private Text yearAndWeekYear = new Text();
    private IntWritable five = new IntWritable(5);
    private IntWritable ten = new IntWritable(10);

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String[] line = value.toString().split(",");
        if (line[0].equals("Year") || line[14].equals("NA") || line[15].equals("NA") || line[16].equals("NA") || line[17].equals("NA")) {

        }
        else if (Integer.parseInt(line[14]) > 15 && Integer.parseInt(line[15]) > 15){
            Calendar calendar = new GregorianCalendar(Integer.parseInt(line[0]), Integer.parseInt(line[1]) - 1, Integer.parseInt(line[2]));
            String mapKey = "Year: " + line[0] + " Week of Year: " + calendar.get(Calendar.WEEK_OF_YEAR) + " Airport: " + line[16];
            yearAndWeekYear.set(mapKey);
            output.collect(yearAndWeekYear, ten);
            mapKey = "Year: " + line[0] + " Week of Year: " + calendar.get(Calendar.WEEK_OF_YEAR) + " Airport: " + line[17];
            yearAndWeekYear.set(mapKey);
            output.collect(yearAndWeekYear, five);

        }
        else if (Integer.parseInt(line[14]) > 15) {
            Calendar calendar = new GregorianCalendar(Integer.parseInt(line[0]), Integer.parseInt(line[1]) - 1, Integer.parseInt(line[2]));
            String mapKey = "Year: " + line[0] + " Week of Year: " + calendar.get(Calendar.WEEK_OF_YEAR) + " Airport: " + line[17];
            yearAndWeekYear.set(mapKey);
            output.collect(yearAndWeekYear, five);

        }

        else if (Integer.parseInt(line[15]) > 15) {
            Calendar calendar = new GregorianCalendar(Integer.parseInt(line[0]), Integer.parseInt(line[1]) - 1, Integer.parseInt(line[2]));
            String mapKey = "Year: " + line[0] + " Week of Year: " + calendar.get(Calendar.WEEK_OF_YEAR) + "Airport: " + line[16];
            yearAndWeekYear.set(mapKey);
            output.collect(yearAndWeekYear, ten);
        }

    }
}