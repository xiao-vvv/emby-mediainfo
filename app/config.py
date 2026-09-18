"""路径常量 + 可在 WebUI 改的设置默认值。
⚠️环境变量不是只在首次启动生效:每次启动都会拿它去补库里的空值(db.py 里那段 insert or ignore + 空值覆盖)。
   也就是说 compose 里留着 AUTH_PASS_HASH,在 WebUI 里「关掉登录」重启后又会被打开 —— 想彻底关掉得把环境变量也删掉。"""
import os
CONFIG_DIR = os.environ.get('CONFIG_DIR', '/config')
DB_PATH = os.path.join(CONFIG_DIR, 'state.sqlite')
OUT_DIR = os.path.join(CONFIG_DIR, 'out')            # 生成的 sidecar json(喂回前暂存)
SECRETS = os.path.join(CONFIG_DIR, 'secrets')
COOKIE_FILE = os.path.join(SECRETS, '115-cookie.txt')

def env(k, d=''): return os.environ.get(k, d)

DEFAULT_SETTINGS = {
    # ---- 运行 ----
    'paused': 1,
    'probe_workers': 3, 'api_interval': 1.0, 'url_batch': 20, 'probe_timeout': 300,
    'feed_batch': 2000, 'feed_interval': 1800, 'refresh_interval': 0.3, 'latency_threshold': 1.0,
    'dir_cache_ttl': 3600, 'max_retries': 3, 'worklist_sync_hours': 24,
    'capture_mode': env('CAPTURE_MODE', 'off'),      # off / with_probe(与媒体信息一起) / after(媒体信息跑完后单独跑)
    'capture_position': 20, 'capture_hdr_tonemap': 1, 'capture_quality': 2,
    'capture_backdrop': 1,          # 电影缺海报时截图同时作背景图(仅当没背景图;与神医 GenerateBackdrop 一致,剧集不生成)
    # 在选中的那一秒里抓 24 帧小图、挑**最静止**的那一帧,再按它的时间点正式截。
    # 这就是 Emby 原生(神医直接用原生逻辑)的做法,wiki 原话:「在那一秒里抓 24 帧,保留最静止的一张」。
    # ⚠️代价是每张多约 2.5 秒(2.4s → 4.8s)。拿真片子盲评下来其实**测不出差别**:
    #   15 集里 7 组两种方法截得不一样,盲评 3 胜 3 负 1 平,两边都没出现黑帧/转场/糊帧。
    #   默认开是因为「和原生一致」本身就是这个项目的前提;嫌慢就关掉,画面不会变差多少。
    # ⚠️别改回按「最锐利」挑:试过,盲评更差(运镜中的特写最锐利,恰恰是该避开的帧)
    'capture_pick_best': 1,
    # ⚠️默认关 = 和神医/Emby 原生完全一致:就用 position% 那一帧,暗就暗。
    # 打开的话撞上黑场/暗场会往后再试两个位置 —— 听着更聪明,但那是我们自己加的一步,
    # 跳走之后落在哪儿没有保证(可能更没意思、甚至剧透)。实测 10 部真人剧里它一次都没触发,
    # 也就是说开着基本没收益,却引入了一个神医不会有的行为差异
    'capture_avoid_dark': 0,
    # ---- 登录(密码只存 scrypt 哈希;留空 = 不需要登录,首页会挂警告条)----
    'auth_user': env('AUTH_USER', 'root'), 'auth_pass_hash': env('AUTH_PASS_HASH', ''), 'auth_epoch': 0,   # AUTH_PASS_HASH 可在首次启动时带进来(值用 /api/auth/set 生成过的那种 scrypt$...)
    # 登录限流是按来源 IP 算的。套了反代时所有人都长成反代那一个 IP(会互相误伤),这时才打开它去读 X-Forwarded-For。
    # ⚠️没有反代却打开 = 谁都能伪造来源绕过限流,所以默认关
    'trust_proxy': int(env('TRUST_PROXY', '0') or 0),
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    # ---- Emby ----
    'emby_api_key': env('EMBY_API_KEY'), 'emby_user_id': env('EMBY_USER_ID'),
    'emby_url': env('EMBY_URL', 'http://127.0.0.1:8096'),        # 同机模式=本容器访问 Emby 的地址;远程模式=从 Emby 主机访问的地址(通常 127.0.0.1:8096)
    'emby_strm_prefix': env('EMBY_STRM_PREFIX', '/strm'),        # Emby 库里 strm 条目 Path 的前缀(Emby 自己看到的路径)
    # ---- 部署模式 ----
    'mode': env('MODE', 'local'),                                # local=与 Emby 同机(挂目录直连) / ssh=Emby 在别的机器
    # 同机模式:容器内挂载路径
    'library_db': env('LIBRARY_DB', '/emby-data/library.db'),
    'strm_root': env('STRM_ROOT', '/strm'),
    'mediainfo_root': env('MEDIAINFO_ROOT', '/mediainfo'),
    # 远程模式:SSH 与对端路径
    'ssh_host': env('SSH_HOST'), 'ssh_port': int(env('SSH_PORT', '22') or 22), 'ssh_user': env('SSH_USER', 'root'),
    'ssh_key': os.path.join(SECRETS, 'id_ed25519'), 'remote_dir': env('REMOTE_DIR', '/opt/emby-mediainfo'),
    'remote_library_db': env('REMOTE_LIBRARY_DB', ''), 'remote_strm_root': env('REMOTE_STRM_ROOT', ''), 'remote_mediainfo_root': env('REMOTE_MEDIAINFO_ROOT', ''),
    # ---- 115 ----
    'strm_115_prefix': env('STRM_115_PREFIX', ''),               # strm 文件内容里 115 根目录的前缀(其后就是 115 网盘路径)
    'exclude_exts': env('EXCLUDE_EXTS', '.ts,.m2ts,.mts'),        # 跳过的容器(与神医默认 MediaInfoExcludeMediaContainers=MpegTs,Ts,M2Ts 一致;神医对这些也不恢复)
    'cd2_115_root': env('CD2_115_ROOT', '/cd2-115'),             # 容器内 115 挂载(仅 ISO 用,可不挂)
}
SECRET_KEYS = {'emby_api_key', 'auth_pass_hash'}   # 哈希也不往外露
# 数值型设置的合法区间(WebUI 能随手填,填出界会把整条流水线搞停:比如 feed_batch=0 → SQL limit -1 → 一次性把全库读进内存)
CLAMPS = {
    'probe_workers': (1, 3),   # 🔴上限 3:115 同时最多 3 条连接是硬规矩,设置页不该能越过(代码里 Slots 也再钳一次)
    'api_interval': (0.5, 60),   # 🔴下限 0.5 = 最多 2 次/秒:115 的 WAF 很敏感,填 0 时贴边到 0.05 等于允许 20 次/秒
    'url_batch': (1, 200), 'probe_timeout': (10, 3600),
    'feed_batch': (1, 5000),   # 上限从 20000 收到 5000:实测 5000 条的 sidecar 就是 98MB 内容 / RSS +130MB,
                               # ssh 模式还要再叠 tar 缓冲和一次 getvalue 拷贝 ≈ 3 倍峰值 —— 20000 能把小内存容器直接 OOM 'feed_interval': (60, 86400), 'refresh_interval': (0.0, 10), 'latency_threshold': (0.05, 60),
    'dir_cache_ttl': (60, 86400), 'max_retries': (1, 20), 'worklist_sync_hours': (0, 720),
    'capture_position': (1, 95), 'capture_quality': (1, 31), 'ssh_port': (1, 65535), 'trust_proxy': (0, 1),
}
STR_MAX = 4096   # 字符串设置的长度上限:user_agent 之类会原样进 HTTP 头,存进去 2MB 的话每次 115 请求都得背着它

SETTING_GROUPS = {
    # ⚠️别把 auth_user 放进来:它只许走 /api/auth/set(要验旧密码),放进设置页会被后端悄悄丢掉,用户以为改了其实没改
    '运行': ['probe_workers', 'api_interval', 'url_batch', 'probe_timeout', 'feed_batch', 'feed_interval', 'refresh_interval', 'latency_threshold', 'dir_cache_ttl', 'max_retries', 'worklist_sync_hours', 'trust_proxy', 'user_agent'],
    '截图': ['capture_mode', 'capture_position', 'capture_hdr_tonemap', 'capture_quality', 'capture_backdrop', 'capture_pick_best', 'capture_avoid_dark'],
    'Emby': ['emby_url', 'emby_api_key', 'emby_user_id', 'emby_strm_prefix'],
    '同机模式': ['library_db', 'strm_root', 'mediainfo_root'],
    '远程模式': ['ssh_host', 'ssh_port', 'ssh_user', 'ssh_key', 'remote_dir', 'remote_library_db', 'remote_strm_root', 'remote_mediainfo_root'],
    # ⚠️别用纯数字当键:JS 里 Object.keys 会把整数样的键排到最前面,这一组会莫名其妙跑到「运行」上面去
    '115 网盘': ['strm_115_prefix', 'cd2_115_root', 'exclude_exts'],
}
def read_cookie():
    return open(COOKIE_FILE).read().strip()
