package cn.clickstream;

import cn.mrbean.WebLogBean;
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
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ClickStreamPageView extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf);

        String inputPath = "hdfs://192.168.136.111:8020/weblog/preoutput";
        String outputPath = "hdfs://192.168.136.111:8020/weblog/pageViewOut";
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.136.111:8020/"), conf);
        if(fileSystem.exists((new Path(outputPath)))) {
            fileSystem.delete(new Path(outputPath),true);
        }
        fileSystem.close();

        job.setJarByClass(ClickStreamPageView.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(inputPath));

        job.setMapperClass(ClickStreamMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WebLogBean.class);


        job.setReducerClass(ClickStreamReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(outputPath));

        boolean res = job.waitForCompletion(true);
        return res ? 0 : 1;
    }
    static class ClickStreamMapper extends Mapper<LongWritable, Text,Text, WebLogBean>{
        Text k = new Text();
        WebLogBean v = new WebLogBean();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\001");
            if(split.length < 9 )
                return;
            //切分的字段set到WebLongBean
            v.set("true".equals(split[0]) ? true : false,split[1],split[2],split[3],split[4],split[5],split[6],split[7],split[8]);
            if(v.isValid()) {
                k.set(v.getRemote_addr());
                context.write(k,v);
            }
        }
    }

   static class ClickStreamReducer extends Reducer<Text,WebLogBean, NullWritable,Text> {
        Text v = new Text();
/*
* reduce阶段接收到的key就是我们的IP
 * 接收到的value就是我们一行行的数据*/
        @Override
        protected void reduce(Text key, Iterable<WebLogBean> values, Context context) throws IOException, InterruptedException {
            ArrayList<WebLogBean> beans = new ArrayList<>();
            try {//先将一个用户所有访问记录排序
            for(WebLogBean bean : values) {
                //通过属性拷贝，每次new一个新对象，避免了bean属性每次覆盖
                WebLogBean webLogBean = new WebLogBean();
                try {
                    BeanUtils.copyProperties(webLogBean,bean);

                } catch (Exception e) {
                    e.printStackTrace();
                }
                beans.add(webLogBean);
            }
            //将bean按时间排序
            Collections.sort(beans, new Comparator<WebLogBean>() {
                @Override
                public int compare(WebLogBean o1, WebLogBean o2) {
                    try {
                        Date d1 = toDate(o1.getTime_local());
                        Date d2 = toDate(o2.getTime_local());
                        if(d1 == null || d2 == null)
                            return 0;
                        return d1.compareTo(d2);
                    } catch (ParseException e) {
                        e.printStackTrace();
                        return 0;
                    }
                }
            });

            //比较相邻两条记录时间差，如果<30分，则记录同属于一个session
            int step = 1;
            String session = UUID.randomUUID().toString();
            for (int i = 0; i < beans.size(); i++) {
                WebLogBean bean = beans.get(i);
                //如果只有一条数据，直接输出
                if (beans.size() == 1) {
                    //设置默认停留60秒
                    v.set(session+"\001" + key.toString() + "\001"+bean.getRemote_user()
                            +"\001" + bean.getTime_local() + "\001"+bean.getRequest() + "\001" +
                            step + "\001" + (60) + "\001" + bean.getHttp_referer() + "\001"+
                            bean.getHttp_user_agent() + "\001" + bean.getBody_bytes_sent() + "\001"+
                            bean.getStatus());
                    context.write(NullWritable.get(),v);
                    break;
                }
                if(i == 0) continue; //不止一条数据，第一条跳过，遍历第二条时在输出

                //近两次的时间差

                long timeDiff = timeDiff(toDate(bean.getTime_local()), toDate(beans.get(i - 1).getTime_local()));


                if(timeDiff < 30*60*1000) {
                    //小于三十分，输出上一次页面访问信息
                    v.set(session+"\001" + key.toString() + "\001"+beans.get(i-1).getRemote_user() + "\001" +
                            beans.get(i-1).getTime_local() + "\001" + beans.get(i-1).getRequest() + "\001" +
                            step + "\001" + (timeDiff/1000) + "\001" + beans.get(i-1).getHttp_referer() + "\001" +
                            beans.get(i-1).getHttp_user_agent() + "\001" + beans.get(i-1).getBody_bytes_sent() + "\001" +
                            beans.get(i-1).getStatus());
                    context.write(NullWritable.get(),v);
                    step++;
                } else {
                    //大于三十分，则输出前前一次页面访问信息，并将step重置
                    v.set(session + "\001" + key.toString() + "\001" + beans.get(i-1).getRemote_user() + "\001" +
                            beans.get(i-1).getTime_local() + "\001"+beans.get(i-1).getRequest() + "\001" + (step)
                            + "\001" + (60) + "\001" + beans.get(i-1).getHttp_referer() + "\001" + beans.get(i-1).getHttp_user_agent()
                            + "\001" + beans.get(i-1).getBody_bytes_sent() + "\001"+beans.get(i-1).getStatus());
                    context.write(NullWritable.get(),v);
                    step = 1;
                    session = UUID.randomUUID().toString();
                }
                if(i == beans.size()-1) {
                    // 设置默认停留时间为60s
                    v.set(session+"\001"+key.toString()+"\001"+bean.getRemote_user() + "\001" +
                            bean.getTime_local() + "\001" + bean.getRequest() + "\001" + step +
                            "\001" + (60) + "\001" + bean.getHttp_referer() + "\001" + bean.getHttp_user_agent()
                            + "\001" + bean.getBody_bytes_sent() + "\001" + bean.getStatus());
                    context.write(NullWritable.get(), v);
                }
            }
            }catch (ParseException e) {
                e.printStackTrace();
            }

        }
        private String toStr(Date date) {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
            return  df.format(date);
        }
        private Date toDate(String timeStr) throws ParseException {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
            return  df.parse(timeStr);
        }
        private long timeDiff(String t1,String t2) throws ParseException {
            Date d1 = toDate(t1);
            Date d2 = toDate(t2);
            return  d1.getTime() - d2.getTime();
        }
        private long timeDiff(Date t1,Date t2) throws ParseException  {
            return  t1.getTime() - t2.getTime();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int  run = ToolRunner.run(configuration,new ClickStreamPageView(),args);
        System.exit(run);
    }

}
