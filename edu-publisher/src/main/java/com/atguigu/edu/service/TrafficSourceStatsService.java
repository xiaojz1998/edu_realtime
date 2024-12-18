package com.atguigu.edu.service;

import com.atguigu.edu.bean.TrafficDurPerSession;
import com.atguigu.edu.bean.TrafficPvPerSession;
import com.atguigu.edu.bean.TrafficSvCt;
import com.atguigu.edu.bean.TrafficUvCt;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Title: TrafficSourceStatsService
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 */
@Service
public interface TrafficSourceStatsService {
    // 1. 获取各来源独立访客数
    List<TrafficUvCt> getUvCt(Integer date);

    // 2. 获取各来源会话数
    List<TrafficSvCt> getSvCt(Integer date);

    // 3. 获取各来源会话平均页面浏览数
    List<TrafficPvPerSession> getPvPerSession(Integer date);

    // 4. 获取各来源会话平均页面访问时长
    List<TrafficDurPerSession> getDurPerSession(Integer date);
}

