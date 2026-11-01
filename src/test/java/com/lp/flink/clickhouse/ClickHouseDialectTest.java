package com.lp.flink.clickhouse;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 方言骨架的自检：URL 匹配、标识符引用、SQL 生成、类型白名单、SPI 注册。
 *
 * <p>这些断言都对应真实故障模式（见父仓库计划 §八 风险 7~9），不是为了凑覆盖率：
 * SPI 文件路径写错、acceptsURL 过宽、upsert 未置空、不支持类型被静默放行，
 * 这四类错误在本地都看不出来，只能在集群提交作业时炸。
 */
public class ClickHouseDialectTest {

    private static final String SPI_RESOURCE =
            "META-INF/services/org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory";

    private final ClickHouseDialectFactory factory = new ClickHouseDialectFactory();

    @Test
    public void acceptsClickHouseUrl() {
        assertTrue(factory.acceptsURL("jdbc:clickhouse://doris05:38123/traccar"));
        assertTrue(factory.acceptsURL("jdbc:clickhouse:http://localhost:8123/default"));
    }

    @Test
    public void rejectsOtherUrls() {
        assertFalse(factory.acceptsURL("jdbc:mysql://meta02:3306/dinky"));
        assertFalse(factory.acceptsURL("jdbc:postgresql://host:5432/db"));
        assertFalse(factory.acceptsURL(""));
        assertFalse(factory.acceptsURL(null));
    }

    @Test
    public void dialectNameAndQuoting() {
        JdbcDialect dialect = factory.create();
        assertEquals("ClickHouse", dialect.dialectName());
        assertEquals("`ods_tc_devices`", dialect.quoteIdentifier("ods_tc_devices"));
        assertEquals("LIMIT 100", dialect.getLimitClause(100));
        assertEquals(
                "com.clickhouse.jdbc.ClickHouseDriver", dialect.defaultDriverName().orElse(""));
    }

    @Test
    public void upsertIsUnsupported() {
        JdbcDialect dialect = factory.create();
        Optional<String> upsert =
                dialect.getUpsertStatement(
                        "ods_tc_devices", new String[] {"id", "name"}, new String[] {"id"});
        assertFalse("ClickHouse 无原生 upsert，必须返回 empty", upsert.isPresent());
    }

    @Test
    public void insertStatementUsesBackticks() {
        JdbcDialect dialect = factory.create();
        String sql = dialect.getInsertIntoStatement("ods_tc_devices", new String[] {"id", "name"});
        assertEquals(
                "INSERT INTO `ods_tc_devices`(`id`, `name`) VALUES (:id, :name)", sql);
    }

    @Test
    public void supportedTypesWhitelist() {
        ClickHouseDialect dialect = new ClickHouseDialect();
        assertTrue(dialect.supportedTypes().contains(org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT));
        assertTrue(dialect.supportedTypes().contains(org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR));
        assertTrue(
                dialect.supportedTypes()
                        .contains(org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE));
        assertFalse(
                "P0 不支持复合类型",
                dialect.supportedTypes().contains(org.apache.flink.table.types.logical.LogicalTypeRoot.ARRAY));
    }

    @Test
    public void validateAcceptsScalarRowType() {
        JdbcDialect dialect = factory.create();
        RowType rowType =
                RowType.of(
                        new LogicalType[] {new BigIntType(), new VarCharType(64)},
                        new String[] {"id", "name"});
        dialect.validate(rowType); // 不应抛异常
    }

    @Test
    public void validateRejectsBinary() {
        JdbcDialect dialect = factory.create();
        RowType rowType =
                RowType.of(new LogicalType[] {new BinaryType(16)}, new String[] {"payload"});
        try {
            dialect.validate(rowType);
            fail("BINARY 应在 validate 阶段被拒绝");
        } catch (ValidationException e) {
            assertTrue(e.getMessage().contains("payload"));
        }
    }

    @Test
    public void spiFileIsPackaged() throws Exception {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(SPI_RESOURCE);
        assertNotNull("jar 内缺少 SPI 服务文件：" + SPI_RESOURCE, in);
        String content;
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            content = reader.lines().collect(Collectors.joining("\n"));
        }
        assertTrue(content.contains(ClickHouseDialectFactory.class.getName()));
    }

    @Test
    public void serviceLoaderDiscoversFactory() {
        boolean found = false;
        Iterator<JdbcDialectFactory> it =
                ServiceLoader.load(JdbcDialectFactory.class).iterator();
        while (it.hasNext()) {
            if (it.next() instanceof ClickHouseDialectFactory) {
                found = true;
                break;
            }
        }
        assertTrue("ServiceLoader 未发现 ClickHouseDialectFactory（SPI 未生效）", found);
    }
}
