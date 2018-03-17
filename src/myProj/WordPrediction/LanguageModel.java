package myProj.WordPrediction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

public class LanguageModel {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        // get the threshold parameter from the configuration
        int threshold;
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            threshold = conf.getInt("threshold", 20);
        }

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //context.write(value, value);
            if (value == null || value.toString().trim().length() == 0) return;
            String line = value.toString().trim();
            String[] parseLine = line.split("\t");
            int count = Integer.valueOf(parseLine[parseLine.length - 1]);
            String[] words = parseLine[0].split("\\s+");

            // if line is null or empty, or incomplete, or count less than threashold
            if (count < threshold || words.length < 2) return;

            // output key and value
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                sb.append(words[i]).append(" ");
            }
            String outputKey = sb.toString().trim();
            String outputVal = words[words.length - 1];

            // this is --> cool = 20
            if (!(outputKey == null || outputKey.length() < 1)) {
                context.write(new Text(outputKey), new Text(outputVal + "=" + count));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

        // get the n parameter from the configuration
        int n;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = new Configuration();
            n = conf.getInt("n", 5);
        }

        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // this is, <boy = 50, girl = 60>
            TreeMap<Integer, List<String>> treeMap = new TreeMap<>();
            for (Text val: values) {
                String curr = val.toString().trim();
                String[] pair = curr.split("=");
                String word = pair[0].trim();
                int count = Integer.valueOf(pair[1].trim());
                if (treeMap.containsKey(count)) {
                    treeMap.get(count).add(word);
                }
                else {
                    treeMap.put(count, new ArrayList<>());
                    treeMap.get(count).add(word);
                }
            }
            Iterator<Integer> iter = treeMap.keySet().iterator();

            for (int i=0; iter.hasNext() && i<n; i++) {
                int valKey = iter.next();
                for (String each: treeMap.get(valKey)) {
                    context.write(new DBOutputWritable(key.toString(), each, valKey), NullWritable.get());
                    i++;
                }
            }

        }
    }

//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf);
//        job.setJarByClass(MRTemplate.class);
//        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
}
