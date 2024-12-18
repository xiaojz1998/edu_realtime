package com.atguigu.edu.service.impl;

import com.atguigu.edu.bean.TrafficKeywords;
import com.atguigu.edu.mapper.TrafficKeywordsMapper;
import com.atguigu.edu.service.TrafficKeywordsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Title: TrafficKeywordsServiceImpl
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 */
@Service
public class TrafficKeywordsServiceImpl implements TrafficKeywordsService {

    @Autowired
    TrafficKeywordsMapper trafficKeywordsMapper;

    @Override
    public List<TrafficKeywords> getKeywords(Integer date) {
        return trafficKeywordsMapper.selectKeywords(date);
    }
}
