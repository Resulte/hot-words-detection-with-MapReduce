# hot-words-detection-with-MapReduce
使用Java MapReduce实现热词发现

## 介绍

热词发现(hot words detection)是通过统计给定数据中的单词出现的频数，找到最常用的单词。

本项目使用Java +  MapRedcue 计算框架，处理给定文本集，挖掘其中使用最频繁的 200 个词汇。

在大数据计算的过程中实现的操作有：

- 将所有大小字母都变成小写 字母;
- 过滤掉所有标点符号和常见的停用词;
- 过滤掉所有数字。

输入的数据为本目录下的text.txt文件。（暮光之城英文文本）

## 算法流程

### 第一步 词频统计

首先，构建一个常用词的哈希表，用于过滤文本中的所有常用停用词：

```java
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
```

```java
// 构建停用词哈希表
stopWordsSet =  new HashSet<>();
for (String stop : stopList) {
  stopWordsSet.add(stop);
}
```

然后写一个Job用于统计文本中所有的词频：

```java
// step1：词频统计
Job job = Job.getInstance(conf, "word count");
```

该Job的Mapper在处理分词的时候可以**将所有文本全部转换为小写字母，并过滤掉所有标点符号和数字等，同时根据单词是否在停用词哈希表中过滤停用词**：

```java
public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    Text word = new Text();
    IntWritable one = new IntWritable(1);
    String line = value.toString().toLowerCase(); // 全部转换为小写字母
    StringTokenizer itr = new StringTokenizer(line, " \t\n\f\"\r\\/.,:;?!@#$%^&*`~|<>()[]{}'+-=1234567890");

    while (itr.hasMoreTokens()) {
      word.set(itr.nextToken());
      // 根据单词是否在停用词哈希表中过滤停用词
      if (stopWordsSet.contains(word.toString())) {
        continue;
      }
      context.write(word, one);
    }
  }
}
```

该Job的Reducer用于统计词频：

```java
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
```

最后将该Job的输出结果输出到一个临时的文件中：

```java
Path tempDir = new Path("hotWordDect-temp-" + Integer.toString( new Random().nextInt(Integer.MAX_VALUE))); //用于输出词频统计的临时目录
FileOutputFormat.setOutputPath(job, tempDir); //设置输出文件路径
```

### 第二步 词频排序

写一个Job用于排序，并且这个Job的输入是上一步的输出：

```java
// step2：词频排序
Job sortJob = Job.getInstance(conf, "sort");
FileInputFormat.addInputPath(sortJob, tempDir);
```

然后创建一个自定义的 Comparator 类对输出结果进行降序排序：

```java
private static class IntWritableDecreasingComparator extends IntWritable.Comparator {

  public int compare(WritableComparable a, WritableComparable b) {
    return -super.compare(a, b);
  }

  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    return -super.compare(b1, s1, l1, b2, s2, l2);
  }
}
```

然后将key 和 value交换，使用这个自定义的 Comparator 类对输出结果中的 key (词频)进行降序排序

```java
// key和value交换
sortJob.setMapperClass(InverseMapper.class);
// 使用这个自定义的 Comparator 类对输出结果中的 key (词频)进行降序排序
sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
```

最后将该Job的输出结果也输出到另外一个临时的文件中：

```java
Path tempDir2 = new Path("hotWordDect-temp2-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE))); //用于输出排序后的临时目录
FileOutputFormat.setOutputPath(sortJob, tempDir2);
sortJob.setOutputKeyClass(IntWritable.class);
sortJob.setOutputValueClass(Text.class);
```

### 第三步 选出前200个高频词汇

写一个Job选出前200个高频词汇，并且这个Job的输入是上一步的输出：

```java
// step3：选出前200个高频词汇
Job topJob = Job.getInstance(conf, "topK");
FileInputFormat.addInputPath(topJob, tempDir2);
```

这个Job的Mapper需要筛选出前200个单词：

```java
private static final int K = 200;public static class TopKMapper extends Mapper<Object, Text, IntWritable, Text>{  int count=0;  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {    StringTokenizer itr = new StringTokenizer(value.toString());    IntWritable a = new IntWritable(Integer.parseInt(itr.nextToken()));    Text b = new Text(itr.nextToken());    if(count < K){      context.write(a, b);      count++;    }  }}
```

最后，安装词频降序排序后输出即可：

```java
// 使用这个自定义的 Comparator 类对输出结果中的 key (词频)进行降序排序
topJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
// 输出
FileOutputFormat.setOutputPath(topJob, new Path(otherArgs[1]));
topJob.setOutputKeyClass(IntWritable.class);
topJob.setOutputValueClass(Text.class);
```

## 实验结果

首先，将输入文本放入HDFS中：

```shell
./bin/hdfs dfs -put text.txt /input
```

然后，执行Jar包：

```shell
./bin/hadoop jar ./HotWordsDetection-1.0-SNAPSHOT.jar /input /output
```



详细输出结果请看当前目录下的output.txt

