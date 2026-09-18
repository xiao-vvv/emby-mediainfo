"""WebUI 登录:会话 cookie(浏览器)+ HTTP Basic(脚本/curl)。密码只存 scrypt 哈希,明文不落盘、不进仓库"""
import os, time, hmac, base64, hashlib, secrets, threading, asyncio, contextlib
from config import CONFIG_DIR
import db

KEY_FILE = os.path.join(CONFIG_DIR, 'secrets', 'session.key')
COOKIE = 'emi_session'
TTL = 30 * 86400                      # 会话 30 天
# n=2^15 → 每次约 32MB 内存、0.1 秒出头;⚠️ 必须显式给 maxmem,OpenSSL 默认上限 32MB,不给会直接抛 memory limit exceeded
SCRYPT = dict(n=2**15, r=8, p=1, dklen=32, maxmem=96 * 1024 * 1024)
MIN_PW = 8
_lock = threading.Lock()
_fails = {}                           # ip → [失败时间戳]
_key_cache = [b'', 0.0]
_basic_cache = {}                     # (header, hash 前缀) → (时刻, 结果):scrypt 很贵,别每个请求都算
_hash_sem = threading.Semaphore(2)    # 同时最多 2 个 scrypt:一次 32MB,并发多了能把容器内存打满

def _secret():
    """签会话用的密钥;文件坏了/空了就重新生成(全员重新登录,但绝不能拿 b'' 当密钥——那样谁都能伪造)"""
    with _lock:
        if _key_cache[0] and time.time() - _key_cache[1] < 60: return _key_cache[0]
        try:
            k = open(KEY_FILE, 'rb').read()
            if len(k) >= 32:
                _key_cache[0], _key_cache[1] = k, time.time(); return k
        except Exception: pass
        os.makedirs(os.path.dirname(KEY_FILE), exist_ok=True)
        k = secrets.token_bytes(32); tmp = KEY_FILE + '.tmp'
        fd = os.open(tmp, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)   # 先写临时文件再原子替换,避免半截文件
        with os.fdopen(fd, 'wb') as f: f.write(k); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, KEY_FILE)
        _key_cache[0], _key_cache[1] = k, time.time()
        return k

def hash_pw(pw: str) -> str:
    """格式 scrypt$n$r$p$salt$hash —— 参数必须写进去:不写的话哪天调参数,所有存量口令会静默失效(9-16 踩过一次,自己都登不进去)"""
    salt = secrets.token_bytes(16)
    with _hash_sem:                    # 算哈希和校验哈希一样吃 32MB,同样要受并发上限管
        h = hashlib.scrypt(pw.encode('utf-8'), salt=salt, **SCRYPT)
    return 'scrypt$%d$%d$%d$%s$%s' % (SCRYPT['n'], SCRYPT['r'], SCRYPT['p'],
                                      base64.b64encode(salt).decode(), base64.b64encode(h).decode())

_DUMMY = hash_pw(secrets.token_hex(16))   # 用户名不对时也跑一遍,免得用耗时问出用户名;口令随机生成(固定值等于给后人埋雷)

def check_pw(pw: str, stored: str) -> bool:
    """按哈希里记的参数来算;老格式(scrypt$salt$hash,没记参数)历史上用过 2^14/2^15,都试一遍"""
    with _hash_sem:
        return _check_pw(pw, stored)

def _check_pw(pw: str, stored: str) -> bool:
    try:
        parts = (stored or '').split('$')
        if parts[0] != 'scrypt': return False
        if len(parts) == 6:
            n, r, p, salt_b64, h_b64 = int(parts[1]), int(parts[2]), int(parts[3]), parts[4], parts[5]
            # 参数是从库里读出来的,必须钳住:n/r 决定内存(256*n*r),p 决定 CPU,写坏了能让一次校验吃几 G 内存或跑几小时。
            # 光钳 n 和 r 的上限不够(2^17 × 16 就是 512MB,乘上并发 2 就是 1G),内存额度要单独算一遍
            if not (2**13 <= n <= 2**17 and 1 <= r <= 16 and 1 <= p <= 4 and 256 * n * r <= 128 * 1024 * 1024): return False
        elif len(parts) == 3:
            # 老格式没记参数:历史上用过 2^14 和 2^15 两档,都试一遍(试错的代价只是多算几十毫秒,试不出来才叫人登不进去)
            salt, want = base64.b64decode(parts[1]), base64.b64decode(parts[2])
            for n in (2**14, 2**15):
                h = hashlib.scrypt(pw.encode('utf-8'), salt=salt, n=n, r=8, p=1, dklen=32, maxmem=256 * n * 8)
                if hmac.compare_digest(h, want): return True
            return False
        else: return False
        h = hashlib.scrypt(pw.encode('utf-8'), salt=base64.b64decode(salt_b64), n=n, r=r, p=p, dklen=32,
                           maxmem=max(96 * 1024 * 1024, 256 * n * r + 1024 * 1024))
        return hmac.compare_digest(h, base64.b64decode(h_b64))
    except Exception:
        return False

# —— 登录相关的三个设置做 2 秒快照 ——
# 🔴每个请求都要读它们,而中间件跑在事件循环上:直接查 sqlite 的话,别人一次长事务就能把整站卡死
#    (实测:后台占着库锁 3 秒,连 /favicon.ico、/login 都要等 2.8 秒)。改口令/退出登录会显式作废快照,
#    所以最多只可能旧 2 秒;拿不到库锁时宁可用旧值也不阻塞。
_SNAP = ['', 'root', '0']
_snap_at = [0.0]
_snap_ready = [False]
_snap_retry = [0.0]      # 探测失败后的退避时刻
NO_LOGIN = '\x00坏了'    # 读不到口令时用的占位哈希:任何口令都对不上它 → 站点关着而不是敞着

def _apply(d):
    """d 是 try_settings 读到的东西。
    🔴auth_pass_hash 读不到(行没了/值坏了)**绝不能**当成「没设密码」——那是把全站敞开。
      运行期就保持上一次的快照;冷启动期就用一个谁都对不上的占位哈希,宁可谁都进不来也不能谁都进得来"""
    if 'auth_pass_hash' not in d:
        if _snap_ready[0]:
            db.log('error', 'auth_pass_hash 读不出来(行没了或值坏了),继续沿用上一次读到的登录状态')
            return False
        _SNAP[:] = [NO_LOGIN, str(d.get('auth_user') or 'root'), str(d.get('auth_epoch') or 0)]
        db.log('error', '🔴 启动时读不到 auth_pass_hash:先按「有口令但谁都对不上」处理,站点保持关闭。'
                        '全新安装不该出现这种情况,检查 /config/state.sqlite 的 settings 表')
    else:
        _SNAP[:] = [d['auth_pass_hash'] or '', str(d.get('auth_user') or 'root'), str(d.get('auth_epoch') or 0)]
    _snap_at[0] = time.time(); _snap_ready[0] = True
    return True

def _snap():
    now = time.time()
    if _snap_ready[0] and (now - _snap_at[0] < 2.0 or now < _snap_retry[0]): return tuple(_SNAP)
    # 冷启动那一次给足超时(模块加载时就先读一次,正常不会落到请求里);之后都是 50 毫秒限时探测
    d = db.try_settings(('auth_pass_hash', 'auth_user', 'auth_epoch'), timeout=0.05 if _snap_ready[0] else 30)
    if d is None:
        # 库正忙 → 用旧值,并且退避半秒。
        # ⚠️不退避的话每个请求都要再去撞一次锁,每次实打实卡住事件循环 50 毫秒 ——
        #   而一个请求要问三次快照,实测吞吐从 2519 掉到 6 rps,连静态文件都跟着卡
        _snap_retry[0] = now + 0.5
        if not _snap_ready[0]: _SNAP[0] = NO_LOGIN   # 一次都没读到过就先按「关着」算,绝不能按「没设密码」算
        return tuple(_SNAP)
    _apply(d)
    return tuple(_SNAP)

def invalidate():
    """口令/用户名/epoch 变了 → 当场重读一遍。
    ⭐在调用方自己的线程里读(改口令、退出登录都是请求处理线程),别把这次阻塞留给事件循环;
      也别只是把缓存标脏 —— 标脏之后万一库正忙,中间件会继续拿旧哈希顶着,旧口令就多活了几秒"""
    _snap_at[0] = 0.0; _snap_retry[0] = 0.0
    try:
        d = db.try_settings(('auth_pass_hash', 'auth_user', 'auth_epoch'), timeout=10)
        if d is not None: _apply(d)
    except Exception: pass

def enabled() -> bool:
    return bool(_snap()[0])

def _bind() -> bytes:
    """会话绑到 用户名+口令哈希+epoch:改口令、改用户名、点退出登录,旧令牌立刻作废"""
    h, u, e = _snap()
    return hashlib.sha256((h + '|' + u + '|' + e).encode('utf-8')).digest()[:8]

def bump_epoch():
    """作废所有已签发的会话"""
    db.set_setting('auth_epoch', int(db.setting('auth_epoch') or 0) + 1); invalidate()

def make_session(user: str) -> str:
    exp = int(time.time()) + TTL
    body = f'{user}|{exp}'.encode('utf-8') + b'|' + _bind()
    return base64.urlsafe_b64encode(body + hmac.new(_secret(), body, hashlib.sha256).digest()).decode()

def valid_session(tok: str) -> bool:
    try:
        raw = base64.urlsafe_b64decode(tok.encode())
        if len(raw) < 40: return False
        body, sig = raw[:-32], raw[-32:]      # ⚠️签名是二进制,里面可能正好有 '|',只能按长度切不能按分隔符切
        if not hmac.compare_digest(sig, hmac.new(_secret(), body, hashlib.sha256).digest()): return False
        head, bind = body[:-8], body[-8:]
        if not hmac.compare_digest(bind, _bind()): return False   # 口令/用户名变过 → 旧会话作废
        user, exp = head.decode('utf-8').rstrip('|').rsplit('|', 1)
        return int(exp) > time.time() and user == _snap()[1]
    except Exception:
        return False

# —— 口令校验的闸门 ——
# ⚠️ 这里踩过两次坑,写清楚为什么是现在这个样子:
#   ① 一开始用「连错 N 次锁 5 分钟」→ 攻击者故意错几次就能把主人关在门外(套反代后大家共用 IP,一锁锁全部)。
#   ② 改成「失败后 await sleep 递增罚时」→ 罚时是并发的,等于只加延迟不限速率,300 个并发 14 秒猜完 300 次,
#      比原来的锁还弱一千倍;而且每次尝试都要跑一次 scrypt,把线程池占满,WebUI 直接卡死。
#   ③ 令牌桶限速(全局每秒 5 次、突发 20)+ 同一来源串行 + 失败递增罚时。
#      令牌不够就直接回 429 且不做任何 scrypt —— 便宜、不针对具体某个人、令牌一直在恢复,主人稍等即可进。
#   ④ ③ 只在 /api/login 上做对了:中间件的 Basic 分支没做前置限流,把「排队 + 罚时」整个放进了线程池,
#      于是 ② 的毛病原样复活 —— 45 个带错密码的 Basic 请求就能占满 40 个线程,WebUI 停摆 70 秒(实测)。
#      现在:排队和罚时都用 asyncio 在事件循环上完成(不占线程),线程池只用来算 scrypt;
#      两条路径共用同一套 令牌桶 → 排队闸 → 罚时 的顺序。
_bucket = {'tokens': 20.0, 'at': time.time()}
_ip_buckets = {}
BUCKET_RATE, BUCKET_MAX = 5.0, 20.0        # 全局:每秒 5 次、突发 20(挡换 IP 刷)
IP_RATE, IP_MAX = 1.0, 5.0                 # 单个来源:每秒 1 次、突发 5
_ip_locks = {}

def _take(b, rate, cap) -> bool:
    now = time.time()
    b['tokens'] = min(cap, b['tokens'] + (now - b['at']) * rate)
    b['at'] = now
    if b['tokens'] < 1.0: return False
    b['tokens'] -= 1.0
    return True

def take_token(ip='?') -> bool:
    """拿一个校验令牌:单个来源和全局各有一个桶。
    ⭐分两层的原因:只有全局桶的话,一个人猛刷就把所有人的额度用光(包括主人);
      只有单 IP 桶的话,换 IP 刷就绕过去了。"""
    with _lock:
        if len(_ip_buckets) > 5000: _ip_buckets.clear()
        ib = _ip_buckets.setdefault(ip, {'tokens': IP_MAX, 'at': time.time()})
        if not _take(ib, IP_RATE, IP_MAX): return False
        if not _take(_bucket, BUCKET_RATE, BUCKET_MAX):
            ib['tokens'] = min(IP_MAX, ib['tokens'] + 1.0)   # 全局没额度,把刚扣的还回去
            return False
        return True

class _Gate:
    __slots__ = ('lock', 'waiting')
    def __init__(self): self.lock = asyncio.Lock(); self.waiting = 0

def _gate(ip: str) -> _Gate:
    g = _ip_locks.get(ip)
    if g is None:
        if len(_ip_locks) > 2000:
            # ⚠️只清空闲的:整个 clear 掉会把正在排队的那把锁换成新的,串行化当场失效
            for k in [k for k, v in list(_ip_locks.items()) if v.waiting == 0]: _ip_locks.pop(k, None)
        g = _ip_locks[ip] = _Gate()
    return g

@contextlib.asynccontextmanager
async def attempt(ip: str):
    """同一来源的口令校验串行化(否则递增罚时会被并发绕过)。
    全程在事件循环上:排队的人不占线程池,只有真正算 scrypt 的那一下才进线程池。
    排队长度有上限(queue_full),否则和攻击者共用一个出口 IP 的主人会排在几十次失败后面"""
    g = _gate(ip); g.waiting += 1
    try:
        async with g.lock: yield
    finally: g.waiting -= 1

def queue_full(ip: str, limit=3) -> bool:
    """该来源已经有太多口令校验在排队 → 直接回 429(便宜,而且几秒后队就空了)。
    计数只在事件循环这一个线程里增减,读到的就是准的"""
    g = _ip_locks.get(ip)
    return bool(g and g.waiting >= limit)

async def penalty(ip: str):
    """一次失败的罚时。⚠️必须 await,不能 time.sleep:睡在线程池里 = 把 WebUI 一起睡死"""
    d = fail_delay(ip); note_fail(ip)
    await asyncio.sleep(d)

def fail_delay(ip: str) -> float:
    """这次失败该罚多久(在同一来源的锁里等,所以对该来源是真的限速)"""
    with _lock:
        now = time.time()
        f = [t for t in _fails.get(ip, []) if now - t < 900]
        return min(8.0, 0.25 * (2 ** min(len(f), 5)))

def note_fail(ip: str):
    with _lock:
        now = time.time()
        _fails[ip] = [t for t in _fails.get(ip, []) if now - t < 900] + [now]
        if len(_fails) > 5000:
            for k in [k for k, v in list(_fails.items()) if not v or now - v[-1] > 900]: _fails.pop(k, None)

def note_ok(ip: str):
    """只清这个来源自己的记录;⚠️别在「缓存命中的 Basic」上调用,否则一个正常轮询的脚本会把同 IP 攻击者的罚时一直清零"""
    with _lock: _fails.pop(ip, None)

_last_unreadable = [0.0]

def current_user() -> str:
    """当前用户名;读不到或值坏了就退回 'root'。
    ⚠️只有用户名能这样兜底 —— 口令哈希不行,它的默认值是空串,而空串的含义是「不用登录」"""
    d = db.try_settings(('auth_user',), timeout=10) or {}
    return str(d.get('auth_user') or 'root')

def verify(user: str, pw: str) -> bool:
    d = db.try_settings(('auth_user', 'auth_pass_hash'), timeout=10)
    # 🔴读不到(行没了)或值坏了(有人直接改过 sqlite)一律拒绝。
    #   这里绝不能退回 DEFAULT_SETTINGS —— auth_pass_hash 的默认是空串 = 全站放行。
    #   也不能直接抛:抛出去就是 500,等于告诉外面「这个口令让服务器炸了」,而且响应时间和正常失败不一样。
    if not d or 'auth_pass_hash' not in d:
        check_pw(pw or '', _DUMMY)          # 照样算一遍,别让耗时暴露出区别
        now = time.time()
        if now - _last_unreadable[0] > 60:  # 被限流挡着也可能每分钟来几十次,别刷屏
            _last_unreadable[0] = now
            db.log('error', 'auth_pass_hash 读不出来(行没了或值坏了),这段时间的登录一律拒绝。'
                            '要重设口令:停容器 → 删掉 settings 里这一行 → 重启')
        return False
    want = str(d.get('auth_user') or 'root')
    stored = str(d.get('auth_pass_hash') or '')
    ok_pw = check_pw(pw or '', stored if (user or '') == want else _DUMMY)   # 用户名不对也照跑,耗时一致
    ok = ok_pw and (user or '') == want
    if ok and len(stored.split('$')) == 3:
        # 老格式(没记参数)登录成功后就地升级:双档试算会让「用户名存在」比不存在慢一截,升级掉就没这个差异了。
        # ⚠️必须「值还是我读到的那个」才写:否则会把同一时刻管理员刚改的新口令覆盖回旧的
        try:
            if db.cas_setting('auth_pass_hash', stored, hash_pw(pw)): db.log('info', '口令哈希已升级为带参数的新格式')
        except Exception: pass
    return ok

def _basic_creds(header: str):
    """把 Basic 头拆成 (user, pw);拆不出来返回 None。
    ⚠️ is_basic 和真正校验必须用同一个解码函数:一个补 '==' 另一个不补的话,
       会出现「进得了闸、进去就抛异常」的输入,等于白送一次罚时"""
    parts = (header or '').split(' ', 1)
    if len(parts) != 2 or parts[0].lower() != 'basic': return None
    try:
        raw = base64.b64decode(parts[1].strip() + '==').decode('utf-8', 'replace')
    except Exception:
        return None
    return tuple(raw.split(':', 1)) if ':' in raw else None

def is_basic(header: str) -> bool:
    """是否真的是一次「带了账号密码」的尝试:光写个 `Basic` 不算,否则谁都能靠它把人锁在门外"""
    return _basic_creds(header) is not None

def basic_cached(header: str):
    """缓存里有结果就返回 True/False,没有返回 None。
    ⭐ 便宜(一次字典查找),所以中间件可以在事件循环上先问它:
      脚本按秒轮询时不必每次都算 scrypt,也就不必每次都花令牌"""
    key = (header, _snap()[0][:24])
    with _lock:
        hit = _basic_cache.get(key)
        return hit[1] if (hit and time.time() - hit[0] < 10) else None

def basic_verify(header: str) -> bool:
    """真的算一次 scrypt。⚠️调用方必须已经拿过令牌、已经进了 attempt() 串行闸"""
    cred = _basic_creds(header)
    ok = bool(cred) and verify(cred[0], cred[1])
    key = (header, _snap()[0][:24])
    with _lock:
        if len(_basic_cache) > 200: _basic_cache.clear()
        _basic_cache[key] = (time.time(), ok)
    return ok

try: _snap()          # ⭐在这里读,而不是等第一个请求:冷读没有短超时,撞上启动期的长事务能把整站冻十秒
except Exception: pass
