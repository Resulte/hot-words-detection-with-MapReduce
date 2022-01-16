package cn.edu.bupt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class HotWordsDect {

    // 常见的停用词
    final private static String[] stopList = new String[]{"very", "ourselves", "am", "doesn", "through", "me", "against", "up", "just", "her", "ours",
            "couldn", "because", "is", "isn", "it", "only", "in", "such", "too", "mustn", "under", "their",
            "if", "to", "my", "himself", "after", "why", "while", "can", "each", "itself", "his", "all", "once",
            "herself", "more", "our", "they", "hasn", "on", "ma", "them", "its", "where", "did", "ll", "you",
            "didn", "nor", "as", "now", "before", "those", "yours", "from", "who", "was", "m", "been", "will",
            "into", "same", "how", "some", "of", "out", "with", "s", "being", "t", "mightn", "she", "again", "be",
            "by", "shan", "have", "yourselves", "needn", "and", "are", "o", "these", "further", "most", "yourself",
            "having", "aren", "here", "he", "were", "but", "this", "myself", "own", "we", "so", "i", "does", "both",
            "when", "between", "d", "had", "the", "y", "has", "down", "off", "than", "haven", "whom", "wouldn",
            "should", "ve", "over", "themselves", "few", "then", "hadn", "what", "until", "won", "no", "about",
            "any", "that", "for", "shouldn", "don", "do", "there", "doing", "an", "or", "ain", "hers", "wasn",
            "weren", "above", "a", "at", "your", "theirs", "below", "other", "not", "re", "him", "during", "which"};

    // 停用词哈希表
    private static Set<String> stopWordsSet;

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Text word = new Text();
            IntWritable one = new IntWritable(1);
            String line = value.toString().toLowerCase(); // 全部转换为小写字母
            StringTokenizer itr = new StringTokenizer(line, " \t\n\f\"\r\\/.,:;?!@#$%^&*`~|<>()[]{}'+-=1234567890");

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (stopWordsSet.contains(word.toString())) {
                    continue;
                }
                context.write(word, one);
            }
        }
    }
    private TreeMap<Long, Long> topK = new TreeMap<Long, Long>();
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            IntWritable result = new IntWritable();
            result.set(sum);
            context.write(key, result);
        }
    }

    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {

        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    private static final int K = 200;
    public static class TopKMapper extends Mapper<Object, Text, IntWritable, Text>{
        int count=0;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            IntWritable a = new IntWritable(Integer.parseInt(itr.nextToken()));
            Text b = new Text(itr.nextToken());

            if(count < K){
                context.write(a, b);
                count++;
            }
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 构建停用词哈希表
        stopWordsSet =  new HashSet<>();
        for (String stop : stopList) {
            stopWordsSet.add(stop);
        }

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: hotWordDect <in> <out>");
            System.exit(2);
        }

        Path tempDir = new Path("hotWordDect-temp-" + Integer.toString( new Random().nextInt(Integer.MAX_VALUE))); //用于输出词频统计的临时目录
        Path tempDir2 = new Path("hotWordDect-temp2-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE))); //用于输出排序后的临时目录

        // step1：词频统计
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(HotWordsDect.class); //设置Jar包的主类
        job.setMapperClass(TokenizerMapper.class); //设置Mapper类
        job.setReducerClass(IntSumReducer.class); //设置Reducer类
        job.setOutputKeyClass(Text.class); //设置输出结果的key数据类型
        job.setOutputValueClass(IntWritable.class); //设置输出结果的value数据类型
        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //设置输入文件路径
        FileOutputFormat.setOutputPath(job, tempDir); //设置输出文件路径
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if(job.waitForCompletion(true)) {
            // step2：词频排序
            Job sortJob = Job.getInstance(conf, "sort");

            sortJob.setJarByClass(HotWordsDect.class);
            FileInputFormat.addInputPath(sortJob, tempDir);
            sortJob.setInputFormatClass(SequenceFileInputFormat.class);
            // key和value交换
            sortJob.setMapperClass(InverseMapper.class);
            sortJob.setNumReduceTasks(1);
            FileOutputFormat.setOutputPath(sortJob, tempDir2);
            sortJob.setOutputKeyClass(IntWritable.class);
            sortJob.setOutputValueClass(Text.class);
            // 使用这个自定义的 Comparator 类对输出结果中的 key (词频)进行降序排序
            sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);

            if (sortJob.waitForCompletion(true)) {
                // step3：选出前200个高频词汇
                Job topJob = Job.getInstance(conf, "topK");

                topJob.setJarByClass(HotWordsDect.class);
                FileInputFormat.addInputPath(topJob, tempDir2);
                //topJob.setInputFormatClass(SequenceFileInputFormat.class);
                topJob.setMapperClass(TopKMapper.class);
                // 降序排序
                topJob.setSortComparatorClass(IntWritableDecreasingComparator.class);

                FileOutputFormat.setOutputPath(topJob, new Path(otherArgs[1]));
                topJob.setOutputKeyClass(IntWritable.class);
                topJob.setOutputValueClass(Text.class);

                System.exit(topJob.waitForCompletion(true) ? 0 : 1);
            }
        }

    }

}
