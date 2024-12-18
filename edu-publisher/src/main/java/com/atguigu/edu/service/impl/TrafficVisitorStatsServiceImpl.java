package com.atguigu.edu.service.impl;

import com.atguigu.edu.bean.TrafficVisitorStatsPerHour;
import com.atguigu.edu.mapper.TrafficVisitorStatsMapper;
import com.atguigu.edu.service.TrafficVisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Title: TrafficVisitorStatsServiceImpl
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 */
@Service
public class TrafficVisitorStatsServiceImpl implements TrafficVisitorStatsService {

    @Autowired
    private TrafficVisitorStatsMapper trafficVisitorStatsMapper;

    // 获取分时流量统计数据
    @Override
    public List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date) {
        return trafficVisitorStatsMapper.selectVisitorStatsPerHr(date);
    }
    // 获取分时流量统计数据

}


