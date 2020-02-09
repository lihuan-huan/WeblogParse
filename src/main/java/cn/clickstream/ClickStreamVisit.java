package cn.clickstream;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class ClickStreamVisit extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int run = ToolRunner.run(conf,new ClickStreamVisit(),args);
        System.exit(run);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf);

        String inputPath = "hdfs://192.168.136.111:8020/weblog/pageViewOut";
        String outputPath = "hdfs://192.168.136.111:8020/weblog/ClickVisitOut";
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.136.111:8020/"), conf);
        if(fileSystem.exists((new Path(outputPath)))) {
            fileSystem.delete(new Path(outputPath),true);
        }
        fileSystem.close();

        job.setJarByClass(ClickStreamVisit.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(inputPath));

        job.setMapperClass(ClickStreamVisitMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageViewsBean.class);


        job.setReducerClass(ClickStreamVisitReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(VisitBean.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(outputPath));

        boolean res = job.waitForCompletion(true);
        return res ? 0 : 1;
    }


    static class ClickStreamVisitMapper extends Mapper<LongWritable, Text,Text,PageViewsBean> {
        PageViewsBean v = new PageViewsBean();
        Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\001");
            int step = Integer.parseInt(split[5]);
            v.set(split[0],split[1],split[2],split[3],split[4],step,split[6],split[7],split[8],split[9]);
            k.set(v.getSession());
            context.write(k,v);
        }
    }

    static class ClickStreamVisitReducer extends Reducer<Text,PageViewsBean, NullWritable,VisitBean> {
        @Override
        protected void reduce(Text key, Iterable<PageViewsBean> values, Context context) throws IOException, InterruptedException {
            ArrayList<PageViewsBean> pageViewsBeans = new ArrayList<>();
            for(PageViewsBean pvBean : values) {
                PageViewsBean bean = new PageViewsBean();
                try {
                    BeanUtils.copyProperties(bean,pvBean);

                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                pageViewsBeans.add(bean);
            }

            Collections.sort(pageViewsBeans, new Comparator<PageViewsBean>() {
                @Override
                public int compare(PageViewsBean o1, PageViewsBean o2) {
                    return o1.getStep() > o2.getStep() ? 1 : -1;
                }
            });

            //取首尾pageview，存于visitbean
            VisitBean visitBean = new VisitBean();
            //获取首记录
            visitBean.setInPage(pageViewsBeans.get(0).getRequest());
            visitBean.setInTime(pageViewsBeans.get(0).getTimestr());
            //获取尾记录
            visitBean.setOutPage(pageViewsBeans.get(pageViewsBeans.size()-1).getRequest());
            visitBean.setOutTime(pageViewsBeans.get(pageViewsBeans.size()-1).getTimestr());
            //获取访问页面次数
            visitBean.setPageVisits(pageViewsBeans.size());
            //来访者ip
            visitBean.setRemote_addr(pageViewsBeans.get(0).getRemote_addr());
            visitBean.setReferal(pageViewsBeans.get(0).getReferal());
            visitBean.setSession(key.toString());
            context.write(NullWritable.get(),visitBean);
        }
    }
}
