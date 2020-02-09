package cn.pre;

import cn.mrbean.WebLogBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;

public class WebLogPreProcess extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WebLogPreProcess.class);

        String inputPath = "hdfs://192.168.136.111:8020/weblog/input";
        String outputPath = "hdfs://192.168.136.111:8020/weblog/preoutput";

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.136.111:8020/weblog/preoutput"), conf);
        if(fileSystem.exists(new Path(outputPath))) {
            fileSystem.delete(new Path(outputPath),true);
        }

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));

        job.setMapperClass(webLogPreProcessMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,new Path(outputPath));
        job.setNumReduceTasks(0);
        boolean res = job.waitForCompletion(true);
        return res ? 0 : 1;
    }

    static class webLogPreProcessMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
        HashSet<String> pages = new HashSet<>();
        Text k = new Text();
        NullWritable v = NullWritable.get();
        /**
         * map阶段的初始化方法
         * 从外部配置文件中加载网站的有用url分类数据 存储到maptask的内存中，用来对日志数据进行过滤
         * 过滤掉我们日志文件当中的一些静态资源，包括js   css  img  等请求日志都需要过滤掉
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            //定义一个集合，集合当中过滤掉我们的一些静态资源
            pages.add("/about");
            pages.add("/black-ip-list/");
            pages.add("/cassandra-clustor/");
            pages.add("/finance-rhive-repurchase/");
            pages.add("/hadoop-family-roadmap/");
            pages.add("/hadoop-hive-intro/");
            pages.add("/hadoop-zookeeper-intro/");
            pages.add("/hadoop-mahout-roadmap/");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            WebLogBean webLogBean = WebLogParser.parser(line);
            if(webLogBean != null) {
                WebLogParser.filtStaticResource(webLogBean,pages);
                k.set(webLogBean.toString());
                context.write(k,v);
            }
        }
    }
        public static void main(String[] args) throws Exception {
            Configuration configuration = new Configuration();
            int  run = ToolRunner.run(configuration,new WebLogPreProcess(),args);
            System.exit(run);
        }

}
