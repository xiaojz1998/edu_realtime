package com.atguigu.edu.mapper;

import com.atguigu.edu.bean.TrafficKeywords;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * Title: TrafficKeywordsMapper
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 */
@Mapper
public interface TrafficKeywordsMapper {
    @Select("select \n" +
            "   keyword,\n" +
            "   sum(keyword_count) keyword_count\n" +
            "from dws_traffic_source_keyword_page_view_window\n" +
            "partition par#{date}\n" +
            "group by  keyword\n" +
            "order by keyword_count desc")
    List<TrafficKeywords> selectKeywords(@Param(value = "date") Integer date);

}

