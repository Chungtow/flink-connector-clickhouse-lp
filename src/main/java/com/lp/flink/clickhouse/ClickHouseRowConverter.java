package com.lp.flink.clickhouse;

import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

/**
 * ClickHouse 行转换器。
 *
 * <p>复用连接器提供的 {@link AbstractJdbcRowConverter}：它已实现
 * {@code toInternal(ResultSet)} 与 {@code toExternal(RowData, FieldNamedPreparedStatement)}
 * 的骨架，并按 {@link org.apache.flink.table.types.logical.LogicalTypeRoot} 分派到具体的
 * getXXX / setXXX。ClickHouse JDBC 驱动（0.6.x）对常见 JDBC 类型（BOOL / 整数 / 浮点 /
 * DECIMAL / String / Date / Timestamp）的读写语义与标准一致，因此 P0 直接沿用，
 * 仅在需要时按类型根做定向覆盖（P1 处理 Array / Map 等复合类型）。
 */
public class ClickHouseRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    public ClickHouseRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public String converterName() {
        return "ClickHouse";
    }
}
