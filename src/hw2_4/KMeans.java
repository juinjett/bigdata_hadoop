package hw2_4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class KMeans {

    private static double cost;
    static int dimension = 58;

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        List<Point> centroids = new ArrayList<>();
//        int dimension = 58;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            // set variable: filePath, to be passed in from cmd line
            String filePath = conf.get("filePath");
            Path pt = new Path("hdfs:" + filePath);
            // Read centroids and create centroids mat
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line = br.readLine();
            while (line != null) {
                // parse string array and create new Points
                String[] parseLine = line.split("\\s+");
                centroids.add(createPoint(parseLine));
                line = br.readLine();
            }

        }

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value == null || value.toString().trim().length() == 0) return;
            String line = value.toString().trim();
            String[] parseLine = line.split("\\s+");
            Point currPoint = createPoint(parseLine);

            // go through ten centroids and find out which one this the closest
            int minIdx = centroids.size();
            double min = Double.MAX_VALUE;
            for (int i=0; i<centroids.size(); i++) {
                double currE = currPoint.euclidian(centroids.get(i));
                if (currE < min) {
                    min = currE;
                    minIdx = i;
                }
            }
            // the minIdx should be less than 10, otherwise sth goes wrong.
            if (minIdx < centroids.size()) {
                cost += min;
                context.write(new Text(String.valueOf(minIdx)), value);
            }
        }

    }

    public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

//        int dimension = 58;
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double[] resCorrds = new double[dimension];
            int count = 0;

            // for each point get summation of coordinates
            for (Text val: values) {
                String line = val.toString().trim();
                String[] parseLine = line.split("\\s+");
                Point currPoint = createPoint(parseLine);
                double[] currCorrds = currPoint.getCorrds();
                for (int i=0; i<dimension; i++) {
                    resCorrds[i] += currCorrds[i];
                }
                count++;
            }

            // generate updated coordinates as string
            StringBuilder sb = new StringBuilder();
            for (int i=0; i<dimension; i++) {
                resCorrds[i] /= count;
                sb.append(resCorrds[i]).append("  ");
            }
            Text res = new Text(sb.toString());
            context.write(res, NullWritable.get());
        }
    }

    public static Point createPoint(String[] strings) {
        double[] corrds = new double[dimension];
        for (int i=0; i<strings.length; i++) {
            corrds[i] = Double.valueOf(strings[i].trim());
        }
        return new Point(corrds);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new Exception("Usage: <input dir> <output dir> <stopwords dir>");
        }

        Configuration conf = new Configuration();
        conf.set("filePath", args[2]);
        Job job = Job.getInstance(conf);
        job.setJarByClass(KMeans.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
