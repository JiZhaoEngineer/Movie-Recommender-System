import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Sum {

    public static class sumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input--> movieA:user1\tsubMultiplication
            //output--> movieA:user1\tsubMultiplication
            String[] line = value.toString().trim().split("\t");
            String movie_user = line[0];
            String subMultiplication = line[1];
            context.write(new Text(movie_user), new DoubleWritable(Double.parseDouble(subMultiplication)));
        }
    }

    public static class sumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            //input--> movieA:user1\tsubMultiplication
            //output--> sum up subMultiplication
            double sum = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(Sum.class);

        job.setMapperClass(sumMapper.class);
        job.setReducerClass(sumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
