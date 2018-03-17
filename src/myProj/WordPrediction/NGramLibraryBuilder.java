package myProj.WordPrediction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NGramLibraryBuilder {

    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        int noGram;
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            noGram = conf.getInt("noGram", 5);
        }

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim().toLowerCase();
            line = line.replace("[^a-z]", " ");
            String[] words = line.split("//s+");

            StringBuilder sb;
            if (words.length < 2) return;
            for (int i=0; i<words.length-1; i++) {
                sb = new StringBuilder();
                sb.append(words[i]);
                for (int j=1; j<noGram && i+j<words.length; j++) {
                    sb.append(" ");
                    sb.append(words[i+j]);
                    context.write(new Text(sb.toString().trim()), new IntWritable(1));
                }
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf);
//        job.setJarByClass(NGramLibraryBuilder.class);
//        job.setMapperClass(TokenizerMapper.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
