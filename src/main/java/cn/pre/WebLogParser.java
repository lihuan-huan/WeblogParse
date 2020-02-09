package cn.pre;

import cn.mrbean.WebLogBean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Locale;

public class WebLogParser {
    public static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
    public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
    public static WebLogBean parser(String line) {
        WebLogBean webLogBean = new WebLogBean();
        String[] s = line.split(" ");
        if(s.length > 11 ) {
            webLogBean.setRemote_addr(s[0]);
            webLogBean.setRemote_user(s[1]);
            String time_local = formatDate(s[3].substring(1));
            if(null == time_local || "".equals(time_local))
                time_local = "-invalid_time-";
            webLogBean.setTime_local(time_local);
            webLogBean.setRequest(s[6]);
            webLogBean.setStatus(s[8]);
            webLogBean.setBody_bytes_sent(s[9]);
            webLogBean.setHttp_referer(s[10]);

            // 如果useragent元素过多，拼接useragent
            if(s.length > 12) {
                StringBuilder sb = new StringBuilder();
                for(int i = 11; i < s.length; i++) {
                    sb.append(s[i]);
                }
                webLogBean.setHttp_user_agent(sb.toString());
            } else {
                webLogBean.setHttp_user_agent(s[11]);
            }

            if(Integer.parseInt(webLogBean.getStatus()) >= 400 ||
            "-invalid_time-".equals(webLogBean.getTime_local())) {
                webLogBean.setValid(false);
            }
        }
        else{
            webLogBean = null;
        }
        return webLogBean;
    }

    public static void filtStaticResource(WebLogBean bean, HashSet<String> pages) {
        if(pages.contains(bean.getRequest()))
            bean.setValid(false);
    }

    public static String formatDate(String time_local) {
        try {
            return df2.format(df1.parse(time_local));
        } catch (ParseException e) {
            return null;
        }
    }

}
