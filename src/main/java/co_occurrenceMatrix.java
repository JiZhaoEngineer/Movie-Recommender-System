import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class co_occurrenceMatrix {

    public static class co_occurrenceMatrixMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input--> userID\tmovie1:rating1,movie2:rating2...
            //output--> movieA:movieB    1
            String[] movie_ratings = value.toString().trim().split("\t");
            if (movie_ratings.length != 2) {//user didn't rating any movie
                return;
            }

            String[] movie_rating = movie_ratings[1].split(",");
            for (int i = 0; i < movie_rating.length; i++) {
                String movieA = movie_rating[i].split(":")[0];
                for (int j = 0; j < movie_rating.length; j++) {
                    String movieB = movie_rating[j].split(":")[0];
                    context.write(new Text(movieA + ":" + movieB), new IntWritable(1));
                }
            }
        }
    }

    public static class co_occurrenceMatrixReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //input--> movieA:movieB\t1
            //output--> movieA:movieB\tsum
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(co_occurrenceMatrix.class);

        job.setMapperClass(co_occurrenceMatrixMapper.class);
        job.setReducerClass(co_occurrenceMatrixReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
