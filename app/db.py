"""SQLite 状态库(断点续跑 + WebUI 数据源)"""
import sqlite3, json, time, threading, os
from config import DB_PATH, DEFAULT_SETTINGS, SECRET_KEYS
_lock = threading.RLock()
AUTH_KEYS = ('auth_pass_hash', 'auth_user', 'auth_epoch')
_bad_settings = set()

def _load(key, raw):
    """解析一个设置值,返回 (值, 是否解析成功)。
    settings 的值是 JSON 编码存的,直接用 sqlite 手改很容易写成裸字符串 —— 从前那样裸 json.loads,
    坏一行就是启动时抛异常(容器起不来)、/api/status 500、feed 循环被兜底 except 抓住后不停重启。
    🔴解析失败**绝不能**给 auth_* 兜默认值:auth_pass_hash 的默认是空串 = 「没设密码,全站放行」,
      改坏库里一行就能把站点敞开。调用方必须按「读不到」而不是「等于默认值」处理。"""
    try: return json.loads(raw), True
    except Exception as e:
        if key not in _bad_settings:      # 每个键只喊一次,不然每秒刷屏
            _bad_settings.add(key)
            print(f'[设置值坏了] {key} 存的不是合法 JSON({str(e)[:60]});'
                  f'原始内容:{str(raw)[:60]!r}。非登录项会按默认值处理。', flush=True)
        return None, False

def connect():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    c = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute('pragma journal_mode=wal'); c.execute('pragma synchronous=normal')
    return c
CONN = connect()
SCHEMA = """
create table if not exists items(
  id integer primary key,            -- Emby ItemId
  kind text, name text, strm text, target text, ext text,
  status text default 'pending',     -- pending/probing/extracted/feeding/done/skipped/failed
  attempts integer default 0, error text, size integer, pickcode text, sha1 text,
  probe_ms integer, streams integer, chapters integer,
  extracted_at real, fed_at real, updated_at real, sidecar text,
  thumb text default 'none', thumb_path text, thumb_err text, priority integer default 0,
  backdrop integer default 0,        -- 0 不需要 / 1 待生成 / 2 已生成 / 3 失败
  thumb_ms integer, thumb_attempts integer default 0,
  sweeps integer default 0,          -- 每日重试放回过几次(防止真坏的文件天天重下)
  size_ok integer default 0          -- 体积是我们自己读出来核实过的(115 清单的数字不准时以它为准)
);
create index if not exists idx_items_status on items(status);
create index if not exists idx_items_ext on items(ext);
create table if not exists settings(key text primary key, value text);
create table if not exists events(id integer primary key autoincrement, ts real, level text, msg text);
create table if not exists metrics(ts real, key text, value real);
create index if not exists idx_metrics on metrics(key, ts);
"""
with _lock:
    CONN.executescript(SCHEMA)
    cols = [r[1] for r in CONN.execute('pragma table_info(items)')]
    for col, typ in (('thumb', "text default 'none'"), ('thumb_path', 'text'), ('thumb_err', 'text'), ('priority', 'integer default 0'),
                     ('backdrop', 'integer default 0'), ('thumb_ms', 'integer'), ('thumb_attempts', 'integer default 0'),
                     ('sweeps', 'integer default 0'), ('size_ok', 'integer default 0')):   # backdrop: 0 不需要/1 待生成/2 已生成/3 失败
        if col not in cols: CONN.execute(f'alter table items add column {col} {typ}')
    # ⚠️调度按 priority desc 排,(status, priority, target) 这个升序索引一次都用不上(explain 实测走的是 idx_items_sched),
    #   留着只是让 几十万行的每次更新多维护一棵 b 树 → 直接删掉
    CONN.execute('drop index if exists idx_items_status_target')
    for ddl in ('create index if not exists idx_items_thumb on items(thumb)',
                'create index if not exists idx_items_sched on items(status, priority desc, target)',
                'create index if not exists idx_items_backdrop on items(backdrop) where backdrop > 0',    # 状态页每 2 秒查一次,不加就是全表扫
                'create index if not exists idx_items_extracted_at on items(extracted_at)',               # 速率统计用
                # 截图相关的几个计数/选择器都是 thumb + status 一起过滤的,只有 thumb 索引要回表几万行:
                # 实测 count(*) where thumb='captured' and status in(...) 最高 825ms —— 那一瞬间所有线程都挡在全局锁后面
                'create index if not exists idx_items_thumb_status on items(thumb, status, ext, thumb_attempts)'):
        CONN.execute(ddl)   # ⚠️这些索引要在 ALTER 补列之后建,不能放进建表脚本(老库那时还没有这些列)
    for k, v in DEFAULT_SETTINGS.items():
        CONN.execute('insert or ignore into settings(key,value) values(?,?)', (k, json.dumps(v)))
        # 环境变量给的非空初值 → 覆盖库里的空值(方便首次用模板/compose 填)
        r = CONN.execute('select value from settings where key=?', (k,)).fetchone()
        cur, ok = _load(k, r[0]) if r else (None, False)
        if r and ok and cur in ('', None) and v not in ('', None): CONN.execute('update settings set value=? where key=?', (json.dumps(v), k))
        elif r and not ok and k not in AUTH_KEYS:   # 值坏了(多半是有人直接改过 sqlite)→ 拿默认值把它救回来,否则容器起不来
            CONN.execute('update settings set value=? where key=?', (json.dumps(v), k))
    CONN.commit()

def setting(key):
    with _lock:
        r = CONN.execute('select value from settings where key=?', (key,)).fetchone()
    if not r: return DEFAULT_SETTINGS.get(key)
    v, ok = _load(key, r[0])
    if ok: return v
    if key in AUTH_KEYS: raise ValueError(f'设置 {key} 的值已损坏,拒绝按默认值处理(那会让站点免登录)')
    return DEFAULT_SETTINGS.get(key)
def set_setting(key, value):
    with _lock:
        CONN.execute('insert or replace into settings(key,value) values(?,?)', (key, json.dumps(value))); CONN.commit()
def cas_setting(key, expect, value):
    """只有当前值仍等于 expect 时才写(compare-and-swap):用于「读出来再改回去」这类容易覆盖并发写的场景"""
    with _lock:
        cur = CONN.execute('select value from settings where key=?', (key,)).fetchone()
        if not cur: return False
        v, ok = _load(key, cur[0])
        if not ok or v != expect: return False      # 值坏了 = 不等于 expect = 不写
        CONN.execute('update settings set value=? where key=?', (json.dumps(value), key)); CONN.commit(); return True

def try_settings(keys, timeout=0.05):
    """限时读几个设置;拿不到库锁返回 None。**返回的是 dict,只包含真的读出来并且解析成功的键**。
    🔴这里绝不能用「读不到就拿默认值顶上」那套:auth_pass_hash 的默认值是空串,
      而空串的含义是「没设密码,全站放行」—— 一次解析失败就能把整个站点悄悄敞开(9-16 亲手写出过这个洞)。
      读不到就是读不到,让调用方自己决定怎么办。
    ⚠️给中间件用的:中间件跑在事件循环上,一次长事务(同步几十万行待办能锁住好几秒)
      就能让整个 WebUI —— 包括登录页和静态文件 —— 一起停摆"""
    if not _lock.acquire(timeout=timeout): return None
    d = {}
    try:
        qs = ','.join('?' * len(keys))
        for r in CONN.execute(f'select key,value from settings where key in ({qs})', tuple(keys)):
            try: d[r['key']] = json.loads(r['value'])
            except Exception: pass      # 值坏了 = 这个键没读到(不是「等于默认值」)
    finally: _lock.release()
    return d

def all_settings(masked=False):
    with _lock:
        rows = [(r['key'], r['value']) for r in CONN.execute('select key,value from settings')]
    d = {}; bad_auth = set()
    for k, raw in rows:
        v, ok = _load(k, raw)
        if ok: d[k] = v
        elif k in AUTH_KEYS: bad_auth.add(k)
    for k in DEFAULT_SETTINGS:
        if k in bad_auth: continue    # 🔴坏掉的 auth 键宁可整个缺席(调用方 KeyError),也不能填成「空串 = 不用登录」
        d.setdefault(k, DEFAULT_SETTINGS[k])
    if masked:
        for k in ('auth_pass_hash', 'auth_epoch'): d.pop(k, None)   # 口令哈希和会话代次都不给前端(前端提交整份设置,带回来会把退出登录撤销掉)
        for k in SECRET_KEYS:
            v = d.get(k)
            if not v or not isinstance(v, str): continue
            d[k] = (v[:4] + '•' * 8 + v[-4:]) if len(v) >= 16 else '•' * 12   # 短密钥别露(原来 'abc' 会显示成 abc••••••••abc)
    return d
def log(level, msg):
    """🔴这个函数绝不能抛异常。
    各个循环线程的兜底 except 里都会调它 —— 磁盘满的时候它一抛,异常就从 except 块里逃出 while 循环,
    线程当场 return,而面板还显示「提取中」、日志页从此再无新行、容器健康检查照样绿。
    实测:满盘时 extract/feed/capture 三个线程同时静默死亡,只有把 db.log 套在 try 里的那个 worker 活了下来。"""
    try:
        with _lock:
            CONN.execute('insert into events(ts,level,msg) values(?,?,?)', (time.time(), level, msg[:2000])); CONN.commit()
    except Exception as e:
        print(f'[写日志失败] {e} —— 原始消息: [{level}] {msg[:300]}', flush=True)
    print(f"[{level}] {msg}", flush=True)
def metric(key, value):
    try:
        with _lock:
            CONN.execute('insert into metrics(ts,key,value) values(?,?,?)', (time.time(), key, value)); CONN.commit()
    except Exception: pass      # 同 log:指标写不进去不该把调用它的线程带走

def quick_check():
    """库有没有坏。⚠️运行中损坏是完全无声的:靠着 page cache 和 WAL 一切照常,
    而每小时那次 wal_checkpoint 还会把干净页往坏文件里写,重启才爆,期间的进度全丢"""
    try:
        with _lock:
            return (CONN.execute('pragma quick_check').fetchone() or ('?',))[0]
    except Exception as e:
        return f'检查本身失败: {e}'
def q(sql, args=()):
    with _lock:
        return CONN.execute(sql, args).fetchall()
def x(sql, args=()):
    with _lock:
        cur = CONN.execute(sql, args); CONN.commit(); return cur.rowcount
def xmany(sql, rows):
    with _lock:
        CONN.executemany(sql, rows); CONN.commit()
def update_item(id, **kw):
    kw['updated_at'] = time.time()
    cols = ', '.join(f'{k}=?' for k in kw)
    x(f'update items set {cols} where id=?', (*kw.values(), id))
def counts():
    return {r['status']: r['n'] for r in q('select status, count(*) n from items group by status')}
