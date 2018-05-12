package Query6;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class Query6Reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, FloatWritable> {

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {

        Float sum = 0f;

        while (values.hasNext()){

            sum += 1;

        }

        output.collect(key, new FloatWritable(sum));

    }

}