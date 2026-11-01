#!/usr/bin/env bash
#===============================================================================
# flink-connector-clickhouse-lp 容器化构建入口（对齐 spark-udf-lp/build.sh）
#
# 用法:
#   bash build.sh [VER]        # VER 默认 1.0.0
#
# 产物:
#   target/flink-connector-clickhouse-dialect-1.17-<VER>.jar   （Maven 产物）
#   build/flink-connector-clickhouse-dialect-1.17-<VER>.jar    （挂载写回宿主，供发布闸门取用）
#
# 前置:
#   docker 可用；首次构建需能访问阿里云 maven 镜像（builder/settings.xml）
#
# 流程:
#   [1/4] builder 镜像不存在则构建（maven:3.9-eclipse-temurin-8 = JDK 8）
#   [2/4] 容器内 mvn clean package（含单测闸门）
#   [3/4] 产物拷到 build/ 并列 sha256
#   [4/4] 产物断言：含 SPI 文件、不夹带 flink connector 类、体积合理
#
# 发布（父仓库执行）:
#   bash ../scripts/flink/release_flink_clickhouse_connector.sh <VER>
#===============================================================================
set -euo pipefail

LP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILDER_DIR="${LP_DIR}/builder"
BUILDER_IMAGE="${BUILDER_IMAGE:-flink-ck-builder:1.0}"
M2_CACHE_VOL="${M2_CACHE_VOL:-flink-ck-m2}"
SETTINGS="${BUILDER_DIR}/settings.xml"
VER="${1:-1.0.0}"

ARTIFACT_ID="flink-connector-clickhouse-dialect"
JAR_NAME="${ARTIFACT_ID}-1.17-${VER}.jar"

[ -f "${LP_DIR}/pom.xml" ] || { echo "ERROR: 未找到 ${LP_DIR}/pom.xml"; exit 1; }
[ -f "${SETTINGS}" ] || { echo "ERROR: 未找到 ${SETTINGS}（构建执行器配置缺失）"; exit 1; }

echo "[1/4] builder 镜像: ${BUILDER_IMAGE}"
if ! docker image inspect "${BUILDER_IMAGE}" >/dev/null 2>&1; then
  docker build -t "${BUILDER_IMAGE}" -f "${BUILDER_DIR}/Dockerfile" "${BUILDER_DIR}"
else
  echo "      已存在，跳过构建（强制重建: docker rmi ${BUILDER_IMAGE}）"
fi

echo "[2/4] 容器化构建 VER=${VER}（含单测）"
docker run --rm \
  -v "${LP_DIR}:/workspace/src" \
  -v "${M2_CACHE_VOL}:/workspace/.m2-cache" \
  -v "${SETTINGS}:/workspace/settings.xml:ro" \
  --user "$(id -u):$(id -g)" \
  -e HOME=/workspace \
  -w /workspace/src \
  "${BUILDER_IMAGE}" \
  mvn -q clean package -Drevision="${VER}" \
    -s /workspace/settings.xml \
    -Dmaven.repo.local=/workspace/.m2-cache

echo "[3/4] 收集产物"
SRC_JAR="${LP_DIR}/target/${JAR_NAME}"
[ -f "${SRC_JAR}" ] || { echo "ERROR: ${SRC_JAR} 未生成"; exit 1; }
mkdir -p "${LP_DIR}/build"
cp -f "${SRC_JAR}" "${LP_DIR}/build/${JAR_NAME}"
( cd "${LP_DIR}/build" && sha256sum "${JAR_NAME}" > "${JAR_NAME}.sha256" )
echo "      build/${JAR_NAME} ($(du -h "${LP_DIR}/build/${JAR_NAME}" | awk '{print $1}'))"
echo "      sha256=$(awk '{print $1}' "${LP_DIR}/build/${JAR_NAME}.sha256")"

echo "[4/4] 产物断言"
python3 - "${LP_DIR}/build/${JAR_NAME}" <<'PY'
import sys
import zipfile

jar = sys.argv[1]
names = zipfile.ZipFile(jar).namelist()
spi = "META-INF/services/org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory"

if spi not in names:
    sys.exit("  ✗ jar 内缺少 SPI 服务文件: %s（JdbcDialectLoader 将找不到方言）" % spi)

leaked = [n for n in names if n.startswith("org/apache/flink/connector/jdbc/")]
if leaked:
    sys.exit("  ✗ jar 内夹带了连接器类 %d 个（会导致 classloader 冲突）: %s" % (len(leaked), leaked[:3]))

ours = [n for n in names if n.startswith("com/lp/flink/clickhouse/") and n.endswith(".class")]
if not ours:
    sys.exit("  ✗ jar 内没有本项目 class")

import os
size_kb = os.path.getsize(jar) / 1024
if size_kb > 512:
    sys.exit("  ✗ jar 体积异常（%.0f KB），疑似把 provided 依赖打了进来" % size_kb)

print("  ✓ SPI 文件就位")
print("  ✓ 未夹带 flink connector 类")
print("  ✓ 本项目 class %d 个，jar %.0f KB" % (len(ours), size_kb))
PY

echo ""
echo "构建完成：build/${JAR_NAME}"
echo "下一步（父仓库发布闸门）:"
echo "  cd .. && bash scripts/flink/release_flink_clickhouse_connector.sh ${VER}"
