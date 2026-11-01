package com.lp.flink.clickhouse;

import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/**
 * ClickHouse 方言（Flink 1.17 / flink-connector-jdbc 3.1.x）。
 *
 * <p>口径对齐 ClickHouse 官方 Flink connector（{@code ClickHouse/flink-connector-clickhouse}）：
 * <ul>
 *   <li><b>insert-only</b>：{@link #getUpsertStatement} 返回空 → 连接器不会走 SELECT+UPDATE 兜底；</li>
 *   <li>因此建表 DDL 中<b>不要声明 PRIMARY KEY</b>，幂等交给 {@code ReplacingMergeTree} + {@code FINAL}/{@code argMax}；</li>
 *   <li>标识符用反引号引用；</li>
 *   <li>不支持的类型在 {@code validate} 阶段显式拒绝（见 {@link #supportedTypes()}），不做隐式转换。</li>
 * </ul>
 *
 * <p>P0 类型范围：BOOLEAN / 整数族 / FLOAT / DOUBLE / DECIMAL(1..76) / CHAR / VARCHAR /
 * DATE / TIMESTAMP(p=0..9，对应 ClickHouse DateTime、DateTime64(0..9))。
 * ARRAY / MAP / MULTISET / ROW 等复合类型留待 P1。
 */
public class ClickHouseDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    /** ClickHouse JDBC 驱动类名（clickhouse-jdbc 0.6.x）。 */
    private static final String DRIVER_NAME = "com.clickhouse.jdbc.ClickHouseDriver";

    /**
     * P0 支持的类型集合。
     *
     * <p>对齐官方 connector 的「无损放宽允许、窄化拒绝」原则：这里只做类型<b>根</b>的白名单，
     * 精度/长度/范围校验交给 ClickHouse 服务端（写入失败会带列名与范围）。
     * 显式不支持：BINARY/VARBINARY（对应 ClickHouse 无二进制列语义）、TIME、TIMESTAMP WITH TIME ZONE、INTERVAL、ARRAY/MAP/ROW/MULTISET。
     */
    private static final Set<LogicalTypeRoot> SUPPORTED_TYPES =
            Collections.unmodifiableSet(
                    EnumSet.of(
                            LogicalTypeRoot.BOOLEAN,
                            LogicalTypeRoot.TINYINT,
                            LogicalTypeRoot.SMALLINT,
                            LogicalTypeRoot.INTEGER,
                            LogicalTypeRoot.BIGINT,
                            LogicalTypeRoot.FLOAT,
                            LogicalTypeRoot.DOUBLE,
                            LogicalTypeRoot.DECIMAL,
                            LogicalTypeRoot.CHAR,
                            LogicalTypeRoot.VARCHAR,
                            LogicalTypeRoot.DATE,
                            LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE));

    @Override
    public String dialectName() {
        return "ClickHouse";
    }

    /**
     * 覆盖基类校验：基类的报错只带类型、不带列名（"The ClickHouse dialect doesn't support type: BINARY(16)."），
     * 对排查没帮助。这里先按<b>列名</b>给出明确错误（对齐官方 connector "指明列名 + 期望类型" 的风格），
     * 再交给基类做 DECIMAL / TIMESTAMP 的精度范围校验。
     */
    @Override
    public void validate(RowType rowType) throws ValidationException {
        for (RowType.RowField field : rowType.getFields()) {
            if (!SUPPORTED_TYPES.contains(field.getType().getTypeRoot())) {
                throw new ValidationException(
                        String.format(
                                "The ClickHouse dialect doesn't support column '%s' of type %s. "
                                        + "Supported type roots: %s.",
                                field.getName(), field.getType(), SUPPORTED_TYPES));
            }
        }
        super.validate(rowType);
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        return SUPPORTED_TYPES;
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new ClickHouseRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    /**
     * ClickHouse 支持反引号与双引号两种标识符引用；这里统一用反引号（与官方 connector 一致）。
     */
    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of(DRIVER_NAME);
    }

    /**
     * ClickHouse 无原生 UPSERT（UPDATE/DELETE 是异步 mutation，不适合作为 sink 路径）。
     * 返回空表示放弃 upsert 能力，连接器退化为 append 写入 —— 与官方 connector 的 insert-only 语义一致。
     */
    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }

    /**
     * ClickHouse {@code Decimal(P, S)}：P ∈ [1, 76]（Decimal256 上限）。
     * 声明范围后，{@code AbstractDialect#validate} 会对超范围的 DECIMAL 直接报错。
     */
    @Override
    public Optional<Range> decimalPrecisionRange() {
        return Optional.of(Range.of(1, 76));
    }

    /**
     * {@code DateTime} 对应精度 0，{@code DateTime64(S)} 支持 S ∈ [0, 9]。
     *
     * <p>注意：Flink 的 TIMESTAMP 默认精度是 6，写入 {@code DateTime64(3)} 列必须显式声明 {@code TIMESTAMP(3)}，
     * 否则会因精度不匹配在写入侧被拒绝。
     */
    @Override
    public Optional<Range> timestampPrecisionRange() {
        return Optional.of(Range.of(0, 9));
    }
}
