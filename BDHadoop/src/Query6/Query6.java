package Query6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Query6 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new Query6(), args);
        System.exit(res);

    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        JobConf job = new JobConf(conf, Query6.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJobName("Query6");

        job.setMapperClass(Query6Mapper.class);
        job.setReducerClass(Query6Reducer.class);

        job.setInputFormat(TextInputFormat.class);

        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        JobClient.runJob(job);
        return 0;
    }

}