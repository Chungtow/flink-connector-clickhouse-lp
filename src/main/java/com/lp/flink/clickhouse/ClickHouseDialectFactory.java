package com.lp.flink.clickhouse;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory;

/**
 * ClickHouse 方言工厂。
 *
 * <p>通过 SPI 注册（服务文件：{@code META-INF/services/org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory}），
 * 由 {@code JdbcDialectLoader} 在规划期用 {@code ServiceLoader} 发现，再按 {@link #acceptsURL(String)} 过滤。
 *
 * <p><b>注意</b>：{@code acceptsURL} 必须精确到 {@code jdbc:clickhouse:} 前缀。匹配过宽会与其他方言同时命中，
 * Flink 会直接抛 "Multiple jdbc dialect factories" 而无法启动作业；匹配不上则抛 "Could not find any jdbc dialect factory"。
 */
public class ClickHouseDialectFactory implements JdbcDialectFactory {

    private static final long serialVersionUID = 1L;

    /** ClickHouse JDBC 驱动（clickhouse-jdbc 0.6.x）的 URL 前缀。 */
    public static final String URL_PREFIX = "jdbc:clickhouse:";

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public JdbcDialect create() {
        return new ClickHouseDialect();
    }
}
