# flink-connector-clickhouse-lp builder 构建镜像
# 说明：
#   1) 固定 JDK 8 —— 与集群 Flink 运行时一致（字节码 major 52）。
#      切勿改成 11/17：本项目的存在理由就是官方 connector 要求 Java 11+。
#   2) 镜像只含 JDK8 + Maven；源码 / .m2 缓存 / settings.xml 全部运行时挂载。
#   3) apt 换阿里云源（仅换源不装包；失败不阻塞构建）。
FROM maven:3.9-eclipse-temurin-8

RUN sed -i 's@deb.debian.org@mirrors.aliyun.com@g; s@security.debian.org@mirrors.aliyun.com@g' /etc/apt/sources.list 2>/dev/null || true; \
    sed -i 's@archive.ubuntu.com@mirrors.aliyun.com@g; s@security.ubuntu.com@mirrors.aliyun.com@g' /etc/apt/sources.list 2>/dev/null || true; \
    rm -rf /var/lib/apt/lists/* 2>/dev/null || true

WORKDIR /workspace/src

# 约定：
#   源码挂载:  -v <repo>:/workspace/src
#   缓存卷:    -v flink-ck-m2:/workspace/.m2-cache
#   仓库配置:  -v <repo>/builder/settings.xml:/workspace/settings.xml:ro
