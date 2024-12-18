package com.atguigu.edu.mapper;

import com.atguigu.edu.bean.TrafficVisitorStatsPerHour;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * Title: TrafficVisitorStatsMapper
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 *    流量分时统计
 */
@Mapper
public interface TrafficVisitorStatsMapper {
    @Select("select\n" +
            "   DATE_FORMAT(stt,'%Y-%m-%d %H:00:00')  hour,\n" +
            "   sum(uv_count) uv_count,\n" +
            "   sum(page_view_count) page_view_count ,\n" +
            "   sum(if(is_new = 1,1,0)) newUvCt\n" +
            "from dws_traffic_vc_ch_ar_visitor_category_page_view_window\n" +
            "partition par#{date}\n" +
            "group by  hour")
    List<TrafficVisitorStatsPerHour> selectVisitorStatsPerHr(Integer date);
}

