package com.atguigu.edu.service;

import com.atguigu.edu.bean.TrafficVisitorStatsPerHour;
import com.atguigu.edu.mapper.TrafficVisitorStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Title: TrafficVisitorStatsService
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 */
@Service
public interface TrafficVisitorStatsService {

    // 获取分时流量数据
    List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date);
}


