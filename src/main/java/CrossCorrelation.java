import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class CrossCorrelation {

    //Pairs
    public static class PairsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final Text wordsMapping = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String[] words = new String[itr.countTokens()];
            int index = 0;
            while (itr.hasMoreTokens()) {
                words[index++] = itr.nextToken();
            }
//            for (String word1 : words) {
//                for (String word2 : words) {
//                        if (word1.compareTo(word2) > 0) {
//                            wordsMapping.set(word1 + " " + word2);
//                            context.write(wordsMapping, one);
//                        }
//                }
//            }
            for (int i = 0; i < words.length; i++) {
                for (int j = i + 1; j < words.length; j++) {
                    if (words[i].compareTo(words[j]) > 0) {
                        wordsMapping.set(words[i] + " " + words[j]);
                    } else {
                        wordsMapping.set(words[j] + " " + words[i]);
                    }
                    context.write(wordsMapping, one);
                }
            }
        }
    }

    public static class PairsReducer extends Reducer<WritableComparable, IntWritable, WritableComparable, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(WritableComparable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    //Stripes
    public static class StripesMapper extends Mapper<Object, Text, Text, MapWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String[] words = new String[itr.countTokens()];
            int index = 0;
            while (itr.hasMoreTokens()) {
                words[index++] = itr.nextToken();
            }

            for (String word1 : words) {
                Map<String, Integer> tmpData = new HashMap<>();
                for (String word2 : words) {
                    if (!word1.equals(word2)) {
                        if (!tmpData.containsKey(word2)) {
                            tmpData.put(word2, 1);
                        } else {
                            int intValue = tmpData.get(word2);
                            tmpData.replace(word2, intValue + 1);
                        }
                    }
                }
                MapWritable resultMap = new MapWritable();
                for (Map.Entry<String, Integer> entryTmpData : tmpData.entrySet()) {
                    resultMap.put(new Text(entryTmpData.getKey()), new IntWritable(entryTmpData.getValue()));
                }
                context.write(new Text(word1), resultMap);
            }

//            for (String word1 : words) {
//                MapWritable data = new MapWritable();
//                for (String word2 : words) {
//                    if (!word1.equals(word2)) {
//                        if (!data.containsKey(new Text(word2))) {
//                            data.put(new Text(word2), new IntWritable(1));
//                        } else {
//                            int intValue = ((IntWritable) data.get(new Text(word2))).get();
//                            data.replace(new Text(word2), new IntWritable(intValue + 1));
//                        }
//                    }
//                }
//                context.write(new Text(word1), data);
//            }
        }
    }

    public static class StripesReducer extends Reducer<WritableComparable, MapWritable, WritableComparable, IntWritable> {

        public void reduce(WritableComparable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable resultData = new MapWritable();
            for (MapWritable tmpData : values) {
                for (Map.Entry<Writable, Writable> entryData : tmpData.entrySet()) {
                    resultData.merge(new Text(key + " " + entryData.getKey().toString()), entryData.getValue(),
                            (oldValue, addValue) -> new IntWritable(((IntWritable) oldValue).get() + ((IntWritable) addValue).get()));
                }
            }
//                    for (Map.Entry<Writable, Writable> map : val.entrySet()) {
//                        if (!data.containsKey(new Text(key.toString() + " " + map.getKey().toString()))) {
//                            data.put(new Text(key + " " +  map.getKey().toString()), map.getValue());
//                        } else {
//                            int intValue = ((IntWritable) data.get(new Text(key + " " + map.getKey().toString()))).get();
//                            data.replace(new Text(key + " " +  map.getKey().toString()),
//                                    new IntWritable(intValue + ((IntWritable)map.getValue()).get()));
//                        }
//                    }

            for (Map.Entry<Writable, Writable> outputData : resultData.entrySet()) {
                context.write((WritableComparable) outputData.getKey(), (IntWritable) outputData.getValue());
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        String job1Name = "cross-correlation";
        String inputPath = args[0];
        String outputPath = args[1] + "/" + job1Name;

        Job job = Job.getInstance(conf, job1Name);
        job.setJarByClass(CrossCorrelation.class);

        if (args[2].equals("pairs")) {
            setConfForPairs(job);
        } else if (args[2].equals("stripes")) {
            setConfForStripes(job);
        } else {
            throw new RuntimeException();
        }
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if (!job.waitForCompletion(true))
            System.exit(1);
    }

    private static void setConfForStripes(Job job) {
        job.setMapperClass(StripesMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setReducerClass(StripesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    }

    private static void setConfForPairs(Job job) {
        job.setMapperClass(PairsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(PairsReducer.class);

        job.setReducerClass(PairsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    }
}
