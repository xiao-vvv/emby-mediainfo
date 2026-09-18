# 以神医的 rffmpeg worker 镜像为底 → ffprobe 与神医插件同一个二进制(7.1.x,含 libbluray)
# ⚠️ 钉住摘要:这个镜像里的 ffprobe 版本决定了字段口径,:latest 哪天换了会静默改变输出
FROM sjtuross/rffmpeg-worker-vulkan@sha256:89d0ae05d389b68059d43b5fc7e43f4b64b3ede0c3b35f20cfd502d532483b02
ENV DEBIAN_FRONTEND=noninteractive PYTHONUNBUFFERED=1 CONFIG_DIR=/config SEVENZ=/usr/local/bin/7zz
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip python3-venv ca-certificates curl xz-utils openssh-client \
 && rm -rf /var/lib/apt/lists/* \
 # 7zz 只给蓝光 ISO 抽 MPLS(补音轨语言)用。⚠️要按目标架构取:基础镜像是多架构的,
 # 写死 x64 的话在 arm64 上装出来的是个跑不起来的二进制(报「Dynamic loader not found」)
 && case "$(dpkg --print-architecture)" in \
      amd64) SEVENZ_ARCH=x64;  SEVENZ_SHA=9a556170350dafb60a97348b86a94b087d97fd36007760691576cac0d88b132b ;; \
      arm64) SEVENZ_ARCH=arm64; SEVENZ_SHA=ea6a2595eba6441e1e60ddaa47d73d849e99ef2ba18d3f386557cdcb9dc9cebd ;; \
      *) echo "不支持的架构 $(dpkg --print-architecture)" >&2; exit 1 ;; \
    esac \
 && curl -fsSL --retry 6 --retry-delay 3 --retry-all-errors --connect-timeout 20 -o /tmp/7z.tar.xz "https://github.com/ip7z/7zip/releases/download/24.09/7z2409-linux-${SEVENZ_ARCH}.tar.xz" \
 && tar -xJf /tmp/7z.tar.xz -C /usr/local/bin 7zz && chmod +x /usr/local/bin/7zz && rm /tmp/7z.tar.xz \
 && echo "${SEVENZ_SHA}  /usr/local/bin/7zz" | sha256sum -c -
COPY requirements.txt /app/requirements.txt
RUN python3 -m venv /venv && /venv/bin/pip install --no-cache-dir -r /app/requirements.txt
COPY app /app
WORKDIR /app
EXPOSE 8000
# 健康检查打 /favicon.ico:它在免登录白名单里,设了口令之后 /api/status 会 401,拿它做检查会一直判不健康
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -fsS -o /dev/null http://127.0.0.1:8000/favicon.ico || exit 1
# --no-proxy-headers:uvicorn 不去解析 X-Forwarded-For。来源 IP 由应用自己决定 ——
# 默认用对端地址,只有在设置里显式打开 trust_proxy 时才认这个头(没反代却认它 = 谁都能伪造来源绕过限流)
ENTRYPOINT ["/venv/bin/uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--no-proxy-headers"]
