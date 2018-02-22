package myProj;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InvertedIndex {

    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        List<String> stopWords = new ArrayList<String>();

        //Keep a list of stop words
        @Override
        protected void setup(Context context) throws IOException {

            // set variable: filePath, to be passed in from cmd line
            Configuration conf = context.getConfiguration();
            String filePath = conf.get("filePath");
            Path pt = new Path("hdfs:" + filePath);//Location of file in HDFS
            // Read stopwords and create stopwords set
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line = br.readLine();
            while (line != null){
                stopWords.add(line.trim().toLowerCase());
                line = br.readLine();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //https://hadoop.apache.org/docs/r2.4.1/api/org/apache/hadoop/mapred/FileSplit.html
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            Text name = new Text(fileName);

            //fast & iterator don't have to load all data
            StringTokenizer tokenizor = new StringTokenizer(value.toString());
            while(tokenizor.hasMoreTokens()) {
                String curWord = tokenizor.nextToken().toString().toLowerCase();
                curWord = curWord.replaceAll("[^a-zA-Z]", "");
                //filter out the stop word
                if(!stopWords.contains(curWord))
                    context.write(new Text(curWord), name);
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            String lastBook = null;
            StringBuilder sb = new StringBuilder();
            int count = 0;
            int threshold = 100;

            //<keyword doc1,doc2,doc3...>
            //values <doc1, doc2, doc2, doc2, doc3,..>
            for (Text value: values) {
                // if no new doc is reached
                if (lastBook != null && value.toString().trim().equals(lastBook)) {
                    count++;
                    continue;
                }

                // if new doc is reached but count less than threshold
                if (lastBook != null && count < threshold) {
                    count = 1;
                    lastBook = value.toString().trim();
                    continue;
                }

                // initiallization, no doc is record
                if (lastBook == null) {
                    lastBook = value.toString().trim();
                    count++;
                    continue;
                }

                sb.append(lastBook);
                sb.append("\t");
                count = 1;
                lastBook = value.toString().trim();
            }

            if (threshold < count) {
                sb.append(lastBook);
            }

            if (!sb.toString().trim().equals("")) {
                context.write(key, new Text(sb.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new Exception("Usage: <input dir> <output dir> <stopwords dir>");
        }

        Configuration conf = new Configuration();
        conf.set("filePath", args[2]);
        Job job = Job.getInstance(conf);
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setNumReduceTasks(3);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
