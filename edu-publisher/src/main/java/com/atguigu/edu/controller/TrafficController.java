package com.atguigu.edu.controller;

import com.atguigu.edu.bean.*;
import com.atguigu.edu.service.TrafficKeywordsService;
import com.atguigu.edu.service.TrafficSourceStatsService;
import com.atguigu.edu.service.TrafficVisitorStatsService;
import com.atguigu.edu.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Title: TrafficController
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 *
 */
@RestController
@RequestMapping("/edu/realtime/traffic")
public class TrafficController {
    // 自动装载关键词统计服务实现类
    @Autowired
    private TrafficKeywordsService trafficKeywordsService;

    // 关键词评分统计请求拦截方法
    @RequestMapping("/keywords")
    public String getKeywords(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateFormatUtil.now();
        }
        List<TrafficKeywords> keywordsList = trafficKeywordsService.getKeywords(date);
        if (keywordsList == null) {
            return "";
        }
        //todo

        List<TrafficKeywords> keywords= trafficKeywordsService.getKeywords(date);

        StringBuilder jsonB = new StringBuilder("{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": [");

        for (int i = 0; i < keywords.size(); i++) {
            TrafficKeywords Keyword = keywords.get(i);
            jsonB.append("{\"name\": \""+Keyword.getKeyword()+"\",\"value\": "+Keyword.getKeywordCount()+"}");
            if(i < keywords.size() - 1){
                jsonB.append(",");
            }
        }

        jsonB.append("]}");
        return jsonB.toString();
    }
    // TODO: 2024/12/17
    // 自动装载访客状态统计服务实现类
    @Autowired
    private TrafficVisitorStatsService trafficVisitorStatsService;

    @RequestMapping("/visitorPerHr")
    public String getVisitorPerHr(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateFormatUtil.now();
        }
        List<TrafficVisitorStatsPerHour> visitorPerHrStatsList = trafficVisitorStatsService.getVisitorPerHrStats(date);
        if (visitorPerHrStatsList == null || visitorPerHrStatsList.size() == 0) {
            return "";
        }

        TrafficVisitorStatsPerHour[] perHrArr = new TrafficVisitorStatsPerHour[24];
        for (TrafficVisitorStatsPerHour trafficVisitorStatsPerHour : visitorPerHrStatsList) {
            Integer hr = trafficVisitorStatsPerHour.getHr();
            perHrArr[hr] = trafficVisitorStatsPerHour;
        }

        String[] hrs = new String[24];
        Long[] uvArr = new Long[24];
        Long[] pvArr = new Long[24];
        Long[] newUvArr = new Long[24];

        for (int hr = 0; hr < 24; hr++) {
            hrs[hr] = String.format("%02d", hr);
            TrafficVisitorStatsPerHour trafficVisitorStatsPerHour = perHrArr[hr];
            if (trafficVisitorStatsPerHour != null) {
                uvArr[hr] = trafficVisitorStatsPerHour.getUv_count();
                pvArr[hr] = trafficVisitorStatsPerHour.getPage_view_count();
                newUvArr[hr] = trafficVisitorStatsPerHour.getNewUvCt();
            } else{
                uvArr[hr] = 0L;
                pvArr[hr] = 0L;
                newUvArr[hr] = 0L;
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\": [\n\"" +
                StringUtils.join(hrs, "\",\"") + "\"\n" +
                "    ],\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"独立访客数\",\n" +
                "        \"data\": [\n" +
                StringUtils.join(uvArr, ",") + "\n" +
                "        ]\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"页面浏览数\",\n" +
                "        \"data\": [\n" +
                StringUtils.join(pvArr, ",") + "\n" +
                "        ]\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"新访客数\",\n" +
                "        \"data\": [\n" +
                StringUtils.join(newUvArr, ",") + "\n" +
                "        ]\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

    // TODO: 2024/12/17

    // 自动装载来源流量统计服务实现类
    @Autowired
    TrafficSourceStatsService trafficSourceStatsService;
    // 1. 独立访客请求拦截方法
    @RequestMapping("/uvCt")
    public String getUvCt(@RequestParam(value = "date", defaultValue = "1") Integer date){
        if (date == 1){
            DateFormatUtil.now();
        }
        List<TrafficUvCt> trafficUvCtList = trafficSourceStatsService.getUvCt(date);
        if (trafficUvCtList == null || trafficUvCtList.size() == 0) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder uvCtValues = new StringBuilder("[");

        for (int i = 0; i < trafficUvCtList.size(); i++) {
            TrafficUvCt trafficUvCt = trafficUvCtList.get(i);
            String source_name = trafficUvCt.getSource_name();
            Long uvCt = trafficUvCt.getUv_count();

        categories.append("\"").append(source_name).append("\"");
        uvCtValues.append("\"").append(uvCt).append("\"");

        if (i < trafficUvCtList.size() - 1) {
            categories.append(",");
            uvCtValues.append(",");
        } else {
            categories.append("]");
            uvCtValues.append("]");
        }
    }
        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"独立访客数\",\n" +
                "        \"data\": " + uvCtValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

    // 2. 会话数请求拦截方法
    @RequestMapping("/svCt")
    public String getPvCt(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            DateFormatUtil.now();
        }
        List<TrafficSvCt> trafficSvCtList = trafficSourceStatsService.getSvCt(date);
        if (trafficSvCtList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder svCtValues = new StringBuilder("[");

        for (int i = 0; i < trafficSvCtList.size(); i++) {
            TrafficSvCt trafficSvCt = trafficSvCtList.get(i);
            String source_name = trafficSvCt.getSource_name();
            Long svCt = trafficSvCt.getTotal_session_count();

            categories.append("\"").append(source_name).append("\"");
            svCtValues.append("\"").append(svCt).append("\"");

            if (i < trafficSvCtList.size() - 1) {
                categories.append(",");
                svCtValues.append(",");
            } else {
                categories.append("]");
                svCtValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"会话数\",\n" +
                "        \"data\": " + svCtValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

    // 3. 各会话浏览页面数请求拦截方法
    @RequestMapping("/pvPerSession")
    public String getPvPerSession(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            DateFormatUtil.now();
        }
        List<TrafficPvPerSession> trafficPvPerSessionList = trafficSourceStatsService.getPvPerSession(date);
        if (trafficPvPerSessionList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder pvPerSessionValues = new StringBuilder("[");

        for (int i = 0; i < trafficPvPerSessionList.size(); i++) {
            TrafficPvPerSession trafficPvPerSession = trafficPvPerSessionList.get(i);
            String source_name = trafficPvPerSession.getSource_name();
            Double pvPerSession = trafficPvPerSession.getPage_view_count();

            categories.append("\"").append(source_name).append("\"");
            pvPerSessionValues.append("\"").append(pvPerSession).append("\"");

            if (i < trafficPvPerSessionList.size() - 1) {
                categories.append(",");
                pvPerSessionValues.append(",");
            } else {
                categories.append("]");
                pvPerSessionValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"会话平均页面浏览数\",\n" +
                "        \"data\": " + pvPerSessionValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

    // 4. 各会话累计访问时长请求拦截方法
    @RequestMapping("/durPerSession")
    public String getDurPerSession(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            DateFormatUtil.now();
        }
        List<TrafficDurPerSession> trafficDurPerSessionList = trafficSourceStatsService.getDurPerSession(date);
        if (trafficDurPerSessionList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder durPerSessionValues = new StringBuilder("[");

        for (int i = 0; i < trafficDurPerSessionList.size(); i++) {
            TrafficDurPerSession trafficDurPerSession = trafficDurPerSessionList.get(i);
            String source_name = trafficDurPerSession.getSource_name();
            Double durPerSession = trafficDurPerSession.getTotal_during_sec();

            categories.append("\"").append(source_name).append("\"");
            durPerSessionValues.append("\"").append(durPerSession).append("\"");

            if (i < trafficDurPerSessionList.size() - 1) {
                categories.append(",");
                durPerSessionValues.append(",");
            } else {
                categories.append("]");
                durPerSessionValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"会话平均页面浏览数\",\n" +
                "        \"data\":" + durPerSessionValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }





}
