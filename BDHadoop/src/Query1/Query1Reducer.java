package Query1;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class Query1Reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, FloatWritable> {

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {

        Float numberOfCancellations = 0f;
        Float sum = 0f;

        while (values.hasNext()){

            sum += 1;
            numberOfCancellations += values.next().get();

        }

        float percentage = numberOfCancellations / sum;

        output.collect(key, new FloatWritable(percentage));

    }

}