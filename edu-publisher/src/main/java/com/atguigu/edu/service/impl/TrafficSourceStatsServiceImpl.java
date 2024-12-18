package com.atguigu.edu.service.impl;

import com.atguigu.edu.bean.TrafficDurPerSession;
import com.atguigu.edu.bean.TrafficPvPerSession;
import com.atguigu.edu.bean.TrafficSvCt;
import com.atguigu.edu.bean.TrafficUvCt;
import com.atguigu.edu.mapper.TrafficSourceStatsMapper;
import com.atguigu.edu.service.TrafficSourceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Title: TrafficSourceStatsServiceImpl
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 */
@Service
public class TrafficSourceStatsServiceImpl implements TrafficSourceStatsService {

    //TODO  自动装载 Mapper 接口实现类
    @Autowired
    TrafficSourceStatsMapper  trafficSourceStatsMapper;
    @Override
    public List<TrafficUvCt> getUvCt(Integer date) {
        return trafficSourceStatsMapper.selectUvCt(date);
    }

    @Override
    public List<TrafficSvCt> getSvCt(Integer date) {
        return trafficSourceStatsMapper.selectSvCt(date);
    }

    @Override
    public List<TrafficPvPerSession> getPvPerSession(Integer date) {
        return trafficSourceStatsMapper.selectPvPerSession(date);
    }

    @Override
    public List<TrafficDurPerSession> getDurPerSession(Integer date) {
        return trafficSourceStatsMapper.selectDurPerSession(date);
    }
}
