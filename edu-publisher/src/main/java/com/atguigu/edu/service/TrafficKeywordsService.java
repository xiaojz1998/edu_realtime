package com.atguigu.edu.service;

import com.atguigu.edu.bean.TrafficKeywords;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Title: TrafficKeywordsService
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 */
@Service
public interface TrafficKeywordsService {
    List<TrafficKeywords> getKeywords (Integer date);
}

