package com.atguigu.edu.mapper;

import com.atguigu.edu.bean.TrafficDurPerSession;
import com.atguigu.edu.bean.TrafficPvPerSession;
import com.atguigu.edu.bean.TrafficSvCt;
import com.atguigu.edu.bean.TrafficUvCt;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * Title: TrafficSourceStatsMapper
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 *
 */
@Mapper
public interface  TrafficSourceStatsMapper {
    // TODO 1. 获取各来源独立访客数
    @Select("select\n" +
            "   source_name,\n" +
            "   sum(uv_count) uv_count\n" +
            "from dws_traffic_vc_ch_ar_visitor_category_page_view_window\n" +
            "partition par#{date} \n" +
            "group by  source_id, source_name \n" +
            "ORDER BY uv_count desc\n")
    List<TrafficUvCt> selectUvCt(@Param("date") Integer date);
    // TODO 2. 获取各来源会话数
    @Select("select\n" +
            "   source_name,\n" +
            "   sum(total_session_count) total_session_count\n" +
            "from dws_traffic_vc_ch_ar_visitor_category_page_view_window\n" +
            "partition par#{date} \n" +
            "group by  source_id, source_name \n" +
            "ORDER BY total_session_count desc")
    List<TrafficSvCt> selectSvCt(@Param("date") Integer date);

    // TODO 3. 获取各来源会话平均页面浏览数
    @Select("select\n" +
            "   source_name,\n" +
            "   sum(page_view_count) /sum(total_session_count) pv_per_session\n" +
            "from dws_traffic_vc_ch_ar_visitor_category_page_view_window\n" +
            "partition par#{date} \n" +
            "group by  source_id, source_name \n" +
            "ORDER BY pv_per_session desc")
    List<TrafficPvPerSession> selectPvPerSession(@Param("date") Integer date);

    // TODO 4. 获取各来源会话平均页面访问时长
    @Select("select\n" +
            "   source_name,\n" +
            "   sum(total_during_time) /sum(total_session_count) dur_per_session\n" +
            "from dws_traffic_vc_ch_ar_visitor_category_page_view_window\n" +
            "partition par#{date} \n" +
            "group by  source_id, source_name \n" +
            "ORDER BY dur_per_session desc")
    List<TrafficDurPerSession> selectDurPerSession(@Param("date") Integer date);

}

