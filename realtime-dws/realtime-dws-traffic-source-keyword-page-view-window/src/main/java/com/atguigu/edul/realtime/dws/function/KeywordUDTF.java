package com.atguigu.edul.realtime.dws.function;

import com.atguigu.edul.realtime.dws.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * Title: KeywordUDTF
 * Create on: 2024/12/16
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 *    自定义UDTF函数
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval (String text){
        for (String keyword : KeywordUtil.analyze(text)) {
            collect(Row.of(keyword));
        }
    }
}
