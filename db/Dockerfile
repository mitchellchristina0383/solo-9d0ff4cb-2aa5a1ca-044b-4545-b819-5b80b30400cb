FROM postgres:16

# csvlog 目录需要 postgres 用户可写：命名卷首次挂载属主是 root，
# 在镜像里先建好目录并 chown，挂载点就会沿用该属主。
RUN mkdir -p /var/log/postgresql && chown postgres:postgres /var/log/postgresql

COPY initdb/ /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/*.sh
