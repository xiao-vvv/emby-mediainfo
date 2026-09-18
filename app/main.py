from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response, PlainTextResponse, JSONResponse, RedirectResponse
from contextlib import asynccontextmanager
from starlette.concurrency import run_in_threadpool
import os, io, csv, json, time, asyncio, threading
import db, hostlib, auth
from pipeline import PIPE
from host import get_host, host_cfg
from config import DEFAULT_SETTINGS, SETTING_GROUPS, SECRET_KEYS, COOKIE_FILE, CLAMPS, STR_MAX

@asynccontextmanager
async def lifespan(app):
    PIPE.start(); db.log('info', '服务启动')
    yield
    PIPE.stop = True
app = FastAPI(title='emby-mediainfo', lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)   # 接口文档页没必要暴露

OPEN_EXACT = {'/login', '/api/login', '/api/logout', '/favicon.ico'}   # 精确匹配,别用前缀(/loginX 也会命中)

_trust_proxy = [0.0, 0, 0.0]     # (上次读到的时刻, 值, 退避到什么时候):中间件在事件循环上,不能每个请求查一次库

def client_ip(request) -> str:
    """限流是按来源算的,所以「来源」得是真的。
    直连时用对端地址;套了反代时所有人都长成反代那一个 IP,限流会互相误伤 ——
    这时才读 X-Forwarded-For 最后一跳(⚠️默认关闭:没有反代却信这个头,等于让人随便伪造来源绕过限流)"""
    now = time.time()
    if now - _trust_proxy[0] > 5 and now >= _trust_proxy[2]:
        d = db.try_settings(('trust_proxy',))
        if d is None: _trust_proxy[2] = now + 0.5      # 库正忙就退避,别每个请求都去撞一次锁(每次都实打实卡住事件循环 50ms)
        else: _trust_proxy[1] = int(d.get('trust_proxy') or 0); _trust_proxy[0] = now
    if _trust_proxy[1]:
        # ⚠️要用 getlist:反代可能是「再加一行 X-Forwarded-For」而不是「往同一行后面接」,
        #   而 headers.get 只给第一行 —— 那行恰恰是客户端自己送来的,等于让人随便挑限流桶
        lines = request.headers.getlist('x-forwarded-for')
        if lines:
            last = lines[-1].split(',')[-1].strip()[:64]
            if last: return last
    return request.client.host if request.client else '?'

@app.middleware('http')
async def gate(request, call_next):
    """没设密码就放行(首页会挂红色警告条);设了就必须带会话 cookie 或 Basic"""
    p = request.url.path
    if '..' in p or '//' in p: return JSONResponse({'detail': 'bad path'}, status_code=400)
    # 🔴跨站检查要放在「没设口令就放行」之前:否则首次安装那段时间里,任何网页都能跨站把口令设成自己的
    if request.method not in ('GET', 'HEAD', 'OPTIONS'):
        sfs = request.headers.get('sec-fetch-site')
        if sfs and sfs not in ('same-origin', 'none'):
            return JSONResponse({'detail': '跨站请求被拒绝'}, status_code=403)
    if p == '/api/login':
        # 声明了大长度的直接拒(便宜的快路径);没声明长度的(chunked,反代不缓冲请求时很常见)由处理函数自己按上限读
        cl = request.headers.get('content-length')
        try:
            if cl is not None and int(cl) > 4096: return JSONResponse({'detail': '请求体过大'}, status_code=413)
        except ValueError: return JSONResponse({'detail': 'content-length 不合法'}, status_code=400)
    if p in OPEN_EXACT or p.startswith('/static/') or not auth.enabled():   # 先看路径再问库:静态文件和登录页一次 sqlite 都不该读
        return await call_next(request)
    has_cookie = bool(request.cookies.get(auth.COOKIE, ''))
    ok = auth.valid_session(request.cookies.get(auth.COOKIE, '')) if has_cookie else False
    if not ok:
        hdr = request.headers.get('authorization', '')
        if auth.is_basic(hdr):                       # 只有真的送了 Basic 才算一次尝试
            cached = auth.basic_cached(hdr)          # 先问缓存:轮询的脚本不该每次都花令牌、更不该每次都算 scrypt
            if cached is not None: ok = cached
            else:
                ip = client_ip(request)
                # 🔴这道前置限流不能省:省掉的那一版里,每个错密码请求都要占一个线程池线程睡最多 8 秒,
                #    45 个并发就能让整个 WebUI 停摆 70 秒(和 /api/login 保持同一套顺序)
                if auth.queue_full(ip) or not auth.take_token(ip):
                    return JSONResponse({'detail': '请求过于频繁,请稍后再试'}, status_code=429)
                async with auth.attempt(ip):         # 排队和罚时都在事件循环上,不占线程
                    ok = await run_in_threadpool(auth.basic_verify, hdr)
                    if not ok: await auth.penalty(ip)
                if ok: auth.note_ok(ip)
    if ok: return await call_next(request)
    if p.startswith('/api/'):
        # 只在对方主动带了凭据时才回 WWW-Authenticate:否则浏览器会弹原生登录框,
        # 用户一填,以后每个同源请求都自动带 Basic,等于把上面那道 CSRF 检查绕过去
        h = {'WWW-Authenticate': 'Basic realm="emby-mediainfo"'} if auth.is_basic(request.headers.get('authorization', '')) else {}
        return JSONResponse({'detail': '未登录'}, status_code=401, headers=h)
    return RedirectResponse('/login', status_code=302)
HERE = os.path.dirname(__file__)
SYNC_LOCK = threading.Lock()        # 待办同步很重(要在 Emby 主机上快照整个 library.db),不许并发
CAP_LOCK = threading.Semaphore(2)   # 手动截图是同步阻塞的,放开了会把整个 API 线程池占死

# ---------- 状态 ----------
@app.get('/api/status')
def status(): return PIPE.status()
@app.get('/api/events')
def events(n: int = 100): return [dict(r) for r in db.q('select * from events order by id desc limit ?', (max(1, min(1000, n)),))]
@app.get('/api/metrics')
def metrics(key: str, hours: float = 6):
    hours = max(0.01, min(720.0, hours))    # 别让 hours 填个巨大的值把整张 metrics 表读出来
    # ⚠️封顶要从「最新」那头截,不是最旧那头:直接 order by ts limit N 砍掉的恰好是刚刚发生的那几十个点
    rows = db.q('select ts,value from metrics where key=? and ts>? order by ts desc limit 20000', (key, time.time()-hours*3600))
    return [dict(r) for r in reversed(rows)]

# ---------- 设置 ----------
@app.get('/api/settings')
def get_settings(): return {'values': db.all_settings(masked=True), 'groups': SETTING_GROUPS, 'secrets': sorted(SECRET_KEYS),
                            'clamps': {k: list(v) for k, v in CLAMPS.items()}}   # 前端自己写一份上下限必然和后端跑偏(实测并发能点到 6,而后端上限是 3)
@app.post('/api/settings')
def set_settings(body: dict):
    changed = []; staged = []; cap_was = db.setting('capture_mode')
    for k, v in body.items():
        if k not in DEFAULT_SETTINGS or k in ('paused', 'auth_pass_hash', 'auth_user', 'auth_epoch'): continue   # 登录相关的都只许走 /api/auth/set 和登录/退出   # paused 走 control 接口;登录口令走 /api/auth/set(要验旧密码)
        if k in SECRET_KEYS and isinstance(v, str) and '•' in v: continue     # 掩码值原样回传 → 不改
        if v is None or (isinstance(v, str) and v.strip() == '' and not isinstance(DEFAULT_SETTINGS[k], str)): continue   # 数字框清空 = 不改
        old = db.setting(k)
        try:
            if isinstance(DEFAULT_SETTINGS[k], bool): v = bool(v)
            elif isinstance(DEFAULT_SETTINGS[k], int) and not isinstance(DEFAULT_SETTINGS[k], bool): v = int(float(v))
            elif isinstance(DEFAULT_SETTINGS[k], float): v = float(v)
            elif isinstance(DEFAULT_SETTINGS[k], str): v = str(v)
        except (TypeError, ValueError, OverflowError): raise HTTPException(400, f'{k} 填的不是数字: {v!r}')
        lo, hi = CLAMPS.get(k, (None, None))
        if lo is not None: v = type(v)(max(lo, min(hi, v)))          # 超出范围就贴边,不接受 0/负数把流水线弄停
        if k == 'capture_mode' and v not in ('off', 'with_probe', 'after'): raise HTTPException(400, f'截图模式只能是 off/with_probe/after: {v!r}')
        if k == 'mode' and v not in ('local', 'ssh'): raise HTTPException(400, f'模式只能是 local/ssh: {v!r}')
        if v == old: continue                          # ⚠️长度检查必须在这句之后:环境变量灌进来的超长值绕过了这道闸,
        if isinstance(v, str) and len(v) > STR_MAX:     #   放在前面的话「保存全部」会永远 400,而且没有任何办法把它改短
            raise HTTPException(400, f'{k} 太长了(最多 {STR_MAX} 字符)')   # user_agent 这种会原样进 HTTP 头
        staged.append((k, v))                      # 先全校验完再落库,免得中途报错留下半套设置
    for k, v in staged: db.set_setting(k, v); changed.append(k)
    if not changed: return get_settings()
    db.log('info', f'设置更新: {", ".join(changed)}')
    # 只在「关 → 开」这一下同步(同步要在 Emby 主机上快照整个 library.db,很重);
    # 原来写的是「新值不是 off」,结果 with_probe ↔ after 之间切一下也会触发一次全量同步
    # ⚠️别再加「库里没有 thumb='pending' 才同步」那个条件:关着截图做同步时也会标缺图行,
    #   于是这个条件恒为假,off→on 这次同步永远不会发生(而它正是为「媒体信息早就有、只缺封面」那批准备的)
    if 'capture_mode' in changed and cap_was == 'off' and db.setting('capture_mode') != 'off':
        # ⚠️先看返回值再说话:撞上正在跑的同步会直接 return {'ok': False},
        #   原来是无条件先写日志「已自动同步」再调用 —— 于是日志说了假话,而「媒体信息早就有、只缺封面」那批一条都没被标记
        r = worklist_sync()
        if r.get('ok') is False: db.log('warn', f"截图已开启,但自动同步没排上队({r.get('msg')})—— 请手动点一次「同步待办清单」,否则「只缺封面」那批不会被标记")
        else: db.log('info', '截图已开启,自动同步一次待办清单以标记缺封面的条目')
    return get_settings()

# ---------- 控制 ----------
@app.post('/api/control/{action}')
def control(action: str):
    if action == 'pause': db.set_setting('paused', 1)
    elif action == 'resume': db.set_setting('paused', 0)
    elif action == 'reset-breaker':
        if not PIPE.client: raise HTTPException(400, '还没有 115 cookie,熔断器无从重置')   # 别掉到最后那个 else 变成 404,那是「没这个动作」的意思
        PIPE.client.tripped_at = None; PIPE.client.reason = None; PIPE.client.cookie_bad = None; PIPE.client.consec_fail = 0
    elif action == 'feed-now':
        if db.setting('paused'): raise HTTPException(400, '已暂停,先恢复运行再入库')
        threading.Thread(target=PIPE.feed_once, daemon=True).start()
    elif action == 'retry-failed':
        # ⚠️批量重试不清 size_ok:那是我们自己从 CD2 读出来核实过的体积,清掉后下一轮又会被 115 清单里的错数字覆盖,
        #   于是又判「读不全」→ 再走一遍慢五倍的 CD2;CD2 要是已经不挂了,这条就永远过不了校验。
        #   想纠正个别核错的,用条目上的单条重试(那是明确的人工动作)
        n = db.x("update items set status='pending', attempts=0 where status='failed'")
        # 顺手救「还是 pending 但 attempts 已经用满」的:调小过 max_retries 就会造出这种谁都不捡的条目
        n += db.x("update items set attempts=0 where status='pending' and attempts >= ?", (int(db.setting('max_retries')),))
        db.log('info', f'失败重试:共放回 {n} 条')
        return {'ok': True, 'n': n, 'msg': f'放回 {n} 条重试'}
    elif action == 'retry-thumbs':
        n = db.x("update items set thumb='pending', thumb_attempts=0, thumb_err=null where thumb='failed' and ext!='.iso'"
                 " and coalesce(thumb_err,'') not like '%排除列表%' and coalesce(thumb_err,'') not like '%Emby 已有封面%'")   # NULL not like … 结果是 NULL,会把这些行漏掉
        m = db.x("update items set backdrop=1 where backdrop=3 and thumb in ('pending','captured')")   # 只给还会再走一遍截图的行重置,否则 backdrop=1 会一直挂在「待生成」里没人捡
        skip = db.q("select count(*) n from items where thumb='failed'")[0]['n']
        db.log('info', f'截图失败重置 {n} 条,背景图失败重置 {m} 条')
        return {'ok': True, 'n': n, 'backdrop': m, 'left': skip,
                'msg': f'重置 {n} 条截图、{m} 条背景图' + (f';还有 {skip} 条按设计不重置(ISO/被排除的容器/Emby 已有封面)' if skip else '')}
    elif action == 'reverify-failed': threading.Thread(target=PIPE.reverify_failed, daemon=True).start()
    else: raise HTTPException(404)
    db.log('info', f'控制: {action}'); return {'ok': True}

# ---------- 条目 ----------
@app.get('/api/items')
def items(status: str = '', q: str = '', page: int = 1, size: int = 50):
    size = max(1, min(500, size)); page = max(1, page)
    where = []; args = []
    if status: where.append('status=?'); args.append(status)
    if q:
        # ⚠️% 和 _ 是 LIKE 的通配符:不转义的话搜一个 % 就把全库都"匹配"出来了
        esc = q.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
        where.append("(name like ? escape '\\' or target like ? escape '\\')"); args += [f'%{esc}%', f'%{esc}%']
    w = ('where ' + ' and '.join(where)) if where else ''
    total = db.q(f'select count(*) n from items {w}', args)[0]['n']
    rows = db.q(f'select id,kind,name,target,ext,status,attempts,error,size,probe_ms,streams,chapters,extracted_at,fed_at,updated_at,thumb,thumb_err from items {w} order by updated_at desc limit ? offset ?', (*args, size, (page-1)*size))
    return {'total': total, 'items': [dict(r) for r in rows]}
@app.post('/api/items/{id}/retry')
def retry(id: int):
    if not db.q('select 1 from items where id=?', (id,)): raise HTTPException(404)   # 不存在的 id 别回 ok,前端会以为放回去了
    # ⚠️正在探测/正在入库的不许插队重置:一改成 pending,调度器会立刻再排一个任务,
    #   两个线程写同一份 sidecar、同一张 jpg,半截文件就这么进了生产 Emby
    n = db.x("update items set status='pending', attempts=0, error=null, priority=1, size_ok=0, updated_at=?"
             " where id=? and status not in ('probing','feeding')", (time.time(), id))
    if not n: raise HTTPException(409, '这条正在处理中(探测/入库),等它跑完再重试')
    return {'ok': True}
@app.post('/api/items/{id}/capture')
def capture_item(id: int):
    """手动对单条截图(测试/重截):取直链→探测→截图→标 captured,下一批入库带上"""
    r = db.q('select * from items where id=?', (id,))
    if not r: raise HTTPException(404)
    it = dict(r[0])
    if it['ext'] == '.iso': raise HTTPException(400, 'ISO 不支持截图')
    if PIPE.client is None: raise HTTPException(400, '无 cookie')
    import prober
    pre = (db.setting('strm_115_prefix') or '').rstrip('/')
    if not pre or not it['target'].startswith(pre + '/'): raise HTTPException(400, 'strm 内容不以「115 路径前缀」开头,先在设置页填对前缀')
    rel = it['target'][len(pre):]
    # 🔴解析目录要放进这把锁里:冷缓存时它会连着打几百个 115 接口(还被最小间隔限着速),
    #    放在锁外面的话,连点几下就是几个线程同时占着 115 客户端,把正常提取饿死
    if not CAP_LOCK.acquire(blocking=False): raise HTTPException(429, '已有手动截图在跑(最多两个),等一下再点')
    try:
        ua = db.setting('user_agent')
        try:
            fid, pc, size, sha = PIPE.client.resolve(rel)
            res = PIPE.client.urls([pc], ua)
        except HTTPException: raise
        except Exception as e:      # 115 那边的错(找不到、熔断、限流)都要说人话,别抛出去变成没头没尾的 500
            import api115; m = api115._scrub(e)[:200]     # ⚠️异常里可能带着带签名的直链,脱敏后再写日志/回前端
            db.log('warn', f'手动截图 {id} 取直链失败: {m}')
            raise HTTPException(502, '115 那边取不到这个文件: ' + m)
        url = res.get(pc) or res.get(str(fid))
        if not url: raise HTTPException(502, '取直链失败: ' + str((res.get('__errors__') or {}).get(pc, '直链缺失')))
        # 🔴和两条自动路径同一道闸:目录树过期/同名撞车时解析到的会是别的文件,
        #    截出来的画面会被当成这个条目的封面传进生产 Emby —— 名字对不上就别截
        m = (res.get('__meta__') or {}).get(pc) or (res.get('__meta__') or {}).get(str(fid)) or {}
        real = str(m.get('name') or ''); want = os.path.basename(it['target'])
        if real and real != want:
            try: PIPE.drop_tree_limited(PIPE.client.category_of(rel))
            except Exception: pass
            raise HTTPException(409, f'解析对不上:115 上这个文件叫「{real[:60]}」,不是「{want[:60]}」。目录树已作废,过一会再试')
        if not PIPE.slots.acquire(timeout=120): raise HTTPException(503, '提取正忙(115 并发已满),稍后再点')
        try:
            try: pr = prober.probe_http(url, ua, int(db.setting('probe_timeout')))
            except Exception as e:
                import api115; m = api115._scrub(e)[:200]
                db.log('warn', f'手动截图 {id} 探测失败: {m}')
                raise HTTPException(502, '探测这个文件失败: ' + m)
            db.x("update items set thumb='pending', thumb_attempts=0, priority=1 where id=?", (id,)); it['thumb'] = 'pending'
            try: PIPE.capture_one(it, url, pr, ua)
            finally: PIPE.inflight.pop(id, None)      # capture_one 会往在途表里登记,手动这条路没人给它销号
        finally: PIPE.slots.release()
    finally: CAP_LOCK.release()
    row = dict(db.q('select thumb,thumb_path,thumb_err from items where id=?', (id,))[0])
    return {'ok': row['thumb'] == 'captured', **row, 'hdr': prober.is_hdr(pr)}
@app.get('/api/export/failed.csv')
def export_failed():
    rows = db.q("select id,kind,target,attempts,error from items where status='failed' order by id")
    buf = io.StringIO(); w = csv.writer(buf)          # 手拼引号会被文件名里的 " 顶破;顺手挡掉 Excel 的公式注入
    w.writerow(['id', 'kind', 'target', 'attempts', 'error'])
    esc = lambda v: ("'" + v) if isinstance(v, str) and v[:1] in ('=', '+', '-', '@') else v
    for r in rows: w.writerow([esc(r['id']), esc(r['kind']), esc(r['target']), r['attempts'], esc(r['error'] or '')])
    return PlainTextResponse(buf.getvalue(), media_type='text/csv', headers={'content-disposition': 'attachment; filename=failed.csv'})

# ---------- Emby 主机 / 连接体检 ----------
@app.get('/api/host/check')
def host_check():
    if db.setting('mode') == 'ssh' and not (db.setting('ssh_host') or '').strip():
        # 不填就去连的话 paramiko 会连到本机,报一句「Authentication failed」把人带偏,还顺手往 known_hosts 写一条空主机
        return {'mode': 'ssh', 'ok': False, 'err': 'SSH 主机还没填(设置 → 远程模式)'}
    try: return {'mode': db.setting('mode'), 'backend': get_host().name, **get_host().check()}
    except Exception as e: return {'mode': db.setting('mode'), 'ok': False, 'err': hostlib._clean(e, host_cfg())[:300]}
@app.post('/api/emby/test')
def emby_test():
    if not (db.setting('emby_url') or '').strip(): return {'ok': False, 'err': 'Emby 地址还没填(设置 → Emby)'}
    if not (db.setting('emby_api_key') or '').strip(): return {'ok': False, 'err': 'Emby API key 还没填(设置 → Emby)'}
    try: return {'ok': True, **get_host().emby_info()}
    except Exception as e: return {'ok': False, 'err': hostlib._clean(e, host_cfg())[:300]}
@app.get('/api/paths/check')
def paths_check():
    """本容器能看到的路径(同机模式的挂载 + ISO 用的 115 挂载)"""
    from pipeline import _exists      # 🔴115 挂载必须用带超时的那个:CD2 挂死时裸 os.path.exists 永远不返回,
    s = db.all_settings(); out = {}   #    而这个接口每次打开设置页都会调 → 一次一个线程,全泄在不可中断的 FUSE 读里
    for key in ('library_db', 'strm_root', 'mediainfo_root', 'cd2_115_root'):
        p = s.get(key) or ''
        ex = bool(p) and _exists(p, 5, ck=f'ui:{key}')   # 独立命名空间:别把设置页这次「顺手看一眼」的结论写进流水线共用的缓存
        out[key] = {'path': p, 'exists': ex, 'readable': ex and os.access(p, os.R_OK), 'writable': ex and os.access(p, os.W_OK)}
    key = s.get('ssh_key') or ''
    pub = ''
    try:
        if key and os.path.exists(key + '.pub'): pub = open(key + '.pub').read().strip()[:4096]
    except OSError as e: pub = f'(读不到公钥: {e.strerror})'    # 权限/损坏不该让整块路径面板变成 500
    out['ssh_key'] = {'path': key, 'exists': bool(key) and os.path.exists(key), 'pubkey': pub}
    out['cookie'] = {'path': COOKIE_FILE, 'exists': os.path.exists(COOKIE_FILE)}
    return out
@app.post('/api/ssh/keygen')
def ssh_keygen():
    """没有 key 时生成一对(公钥显示给用户去加到 Emby 主机)"""
    import subprocess
    key = db.setting('ssh_key') or ''
    if not key.startswith('/'): raise HTTPException(400, '先在设置里把「私钥路径」填成一个绝对路径')   # 空字符串会让下面 dirname('') → makedirs 直接 500
    os.makedirs(os.path.dirname(key), exist_ok=True)
    if os.path.exists(key): raise HTTPException(400, 'key 已存在')
    subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-C', 'emby-mediainfo', '-f', key], check=True)
    os.chmod(key, 0o600); return {'pubkey': open(key + '.pub').read().strip()}

# ---------- 待办清单 ----------
@app.post('/api/worklist/sync')
def worklist_sync():
    if not SYNC_LOCK.acquire(blocking=False): return {'ok': False, 'msg': '已有同步在跑'}
    def job():
        try:
            db.log('info', '同步待办清单:在 Emby 主机做库快照…')
            rows, meta = get_host().fetch_worklist()
            ids_now = set(r['id'] for r in rows if not r.get('has_mediainfo'))
            unread = int(meta.get('unreadable') or 0); total_now = int(meta.get('total_strm_items') or 0)
            last_total = int(db.setting('_last_strm_total') or 0)
            bad = (not rows) or unread > max(50, 0.05 * max(1, len(rows) + unread)) or (last_total and total_now < last_total * 0.8) \
                  or bool(meta.get('unreadable_truncated'))   # 读不到的 id 列表被截断 → 判不出谁是真没了,这次别改状态
            if bad:
                db.log('error', f'待办同步: 清单异常(候选 {len(rows)}, 读不到 strm {unread}, 库里 strm {total_now} 上次 {last_total}),本次不标记「已有媒体信息」')
            db.set_setting('_last_strm_total', total_now)   # 基线照记:库真的变小了就以新值为准,免得一次误判把守卫永久锁死
            db.xmany("insert or ignore into items(id,kind,name,strm,target,ext,status,thumb,backdrop,error,updated_at) values(?,?,?,?,?,?,?,?,?,?,?)",
                     [(r['id'], r['kind'], r['name'], r['strm'], r['target'], os.path.splitext(r['target'])[1].lower(),
                       'skipped' if r.get('has_mediainfo') else 'pending', 'pending' if r.get('need_thumb') else 'none', 1 if r.get('need_backdrop') else 0,
                       'Emby 已有媒体信息,只缺封面截图' if r.get('has_mediainfo') else None, time.time()) for r in rows])
            # 已在库里的条目:补 thumb/背景图需求(缺图→pending)
            need = [r['id'] for r in rows if r.get('need_thumb')]     # 缺封面标记只依赖「读到的行」,不受清单异常守卫影响
            for c in range(0, len(need), 900):
                part = need[c:c+900]; db.x(f"update items set thumb='pending' where thumb='none' and id in ({','.join('?'*len(part))})", part)
            need_bd = [r['id'] for r in rows if r.get('need_backdrop')]
            for c in range(0, len(need_bd), 900):
                part = need_bd[c:c+900]; db.x(f"update items set backdrop=1 where backdrop=0 and id in ({','.join('?'*len(part))})", part)
            db.x("update items set thumb='failed', thumb_err='ISO 暂不支持截图' where thumb='pending' and ext='.iso'")
            db.x("update items set error='Emby 已有媒体信息,只缺封面截图' where status='skipped' and error is null and thumb='pending'")
            pend = [r['id'] for r in db.q("select id from items where status in ('pending','failed')")]
            unread_ids = set(meta.get('unreadable_ids') or [])     # strm 读不到的:这次判不了,别当成「Emby 已有媒体信息」
            gone = [] if bad else [i for i in pend if i not in ids_now and i not in unread_ids]
            for c in range(0, len(gone), 900):
                part = gone[c:c+900]
                db.x(f"update items set status='skipped', error='Emby 已有媒体信息(同步时发现)' where id in ({','.join('?'*len(part))})", part)
            db.log('info', f"待办同步完成: 无媒体信息 {meta.get('no_mediainfo')} / strm 条目 {meta.get('total_strm_items')} / 读不到 strm {meta.get('unreadable')}; 候选 {len(rows)}, 缺封面 {len(need)}(其中电影缺背景图 {len(need_bd)}), 标记跳过 {len(gone)}; 主机耗时 {meta.get('seconds')}s")
        except Exception as e: db.log('error', f'待办同步失败: {str(e)[:400]}')
        finally:
            db.set_setting('_last_sync_at', time.time())   # 谁发起的都算数:自动同步读这个,才不会在手动同步刚跑完之后又跑一遍
            SYNC_LOCK.release()
    try: threading.Thread(target=job, daemon=True).start()
    except Exception as e:
        SYNC_LOCK.release(); db.log('error', f'待办同步线程起不来: {str(e)[:200]}'); raise HTTPException(500, '同步线程起不来')
    return {'ok': True, 'msg': '后台同步中,看日志'}

# ---------- cookie ----------
@app.get('/api/cookie')
def cookie_info():
    try: mt = os.path.getmtime(COOKIE_FILE); head = open(COOKIE_FILE).read(40)
    except Exception: mt = None; head = ''
    return {**PIPE.cookie, 'file_mtime': mt, 'preview': (head.split(';')[0] if head else '')}
@app.post('/api/cookie/check')
def cookie_check(): return PIPE.check_cookie()
@app.post('/api/cookie')
def cookie_set(body: dict):
    ck = (body.get('cookie') or '').strip()
    if 'UID=' not in ck or 'CID=' not in ck: raise HTTPException(400, 'cookie 至少要含 UID= 和 CID=')
    try: return PIPE.replace_cookie(ck)
    except ValueError as e: raise HTTPException(400, str(e))

# ---------- 静态 ----------
@app.get('/favicon.ico')
def favicon():
    svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="14" fill="#0a84ff"/><path d="M18 40 L28 22 L36 34 L42 28 L48 40 Z" fill="#fff"/></svg>'
    return Response(svg, media_type='image/svg+xml')
app.mount('/static', StaticFiles(directory=os.path.join(HERE, 'static')), name='static')
LOGIN_HTML = """<!doctype html><html lang=zh><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>登录 · emby-mediainfo</title><style>
:root{color-scheme:light dark}*{box-sizing:border-box}
body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;background:#0b0b0d;color:#eee;
 font:15px/1.5 -apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif}
@media (prefers-color-scheme:light){body{background:#f2f2f7;color:#111}}
form{width:min(92vw,340px);padding:26px 22px;border-radius:18px;background:rgba(255,255,255,.06);backdrop-filter:blur(20px) saturate(180%);
 border:1px solid rgba(255,255,255,.1);box-shadow:0 12px 40px rgba(0,0,0,.35)}
@media (prefers-color-scheme:light){form{background:rgba(255,255,255,.72);border-color:rgba(0,0,0,.06)}}
h1{margin:0 0 4px;font-size:19px}p.s{margin:0 0 18px;opacity:.6;font-size:13px}
label{display:block;margin-bottom:12px}span{display:block;font-size:12px;opacity:.65;margin-bottom:5px}
input{width:100%;padding:11px 12px;font-size:16px;border-radius:11px;border:1px solid rgba(128,128,128,.35);background:rgba(128,128,128,.12);color:inherit}
button{width:100%;margin-top:6px;padding:12px;font-size:16px;border:0;border-radius:11px;background:#0a84ff;color:#fff;font-weight:600;cursor:pointer}
button:active{transform:scale(.985)}.e{margin-top:12px;color:#ff6961;font-size:13px;min-height:18px}
</style></head><body><form id=f onsubmit="go(event)">
<h1>emby-mediainfo</h1><p class=s>请先登录</p>
<label><span>用户名</span><input name=u autocomplete=username autocapitalize=off autocorrect=off required></label>
<label><span>密码</span><input name=p type=password autocomplete=current-password required></label>
<button>登录</button><div class=e id=e></div></form><script>
async function go(ev){ev.preventDefault();const f=ev.target,e=document.getElementById('e'),b=f.querySelector('button');
 if(b.disabled)return; b.disabled=true;const t=b.textContent;b.textContent='登录中…';e.textContent='';
 try{
  const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},
   body:JSON.stringify({user:f.u.value,password:f.p.value})});
  if(r.ok){location.href='/';return}
  const j=await r.json().catch(()=>({}));e.textContent=(j&&j.detail)||('登录失败 '+r.status);
 }catch(err){e.textContent='连不上服务:'+err.message}
 finally{b.disabled=false;b.textContent=t}}
</script></body></html>"""

@app.get('/login')
def login_page():
    if not auth.enabled(): return RedirectResponse('/', status_code=302)
    return Response(LOGIN_HTML, media_type='text/html', headers={'Cache-Control': 'no-store'})

@app.post('/api/login')
async def login(request: Request):
    ip = client_ip(request)
    async def read_body():
        raw = b''                                    # 自己读,顺手封顶:chunked 请求没有 content-length,不能靠中间件挡
        async for chunk in request.stream():
            raw += chunk
            if len(raw) > 4096: raise HTTPException(413, '请求体过大')
        return raw
    try: raw = await asyncio.wait_for(read_body(), timeout=15)   # 挡 slowloris:一点点吐字节把连接占住
    except asyncio.TimeoutError: raise HTTPException(408, '请求体发送超时')
    try: body = json.loads(raw or b'{}')
    except Exception: raise HTTPException(400, '请求体不是合法 JSON')
    if not isinstance(body, dict): raise HTTPException(400, '请求体应当是个对象')
    if not auth.enabled(): return {'ok': True, 'msg': '未设置密码,无需登录'}
    u, pw = str(body.get('user') or ''), str(body.get('password') or '')
    if not u and not pw: raise HTTPException(400, '请填用户名和密码')   # 空请求不该被算成一次失败尝试
    if auth.queue_full(ip) or not auth.take_token(ip): raise HTTPException(429, '请求过于频繁,请稍后再试')   # 队满或没令牌都直接拒,不做 scrypt
    async with auth.attempt(ip):                     # 同一来源串行:递增罚时只有在串行时才是真的限速
        ok = await run_in_threadpool(auth.verify, u, pw)
        if not ok: await auth.penalty(ip)            # 罚时在事件循环上等,不占线程池
    if not ok: raise HTTPException(401, '用户名或密码不对')
    auth.note_ok(ip); db.log('info', f'登录成功: {ip}')
    r = JSONResponse({'ok': True})
    r.set_cookie(auth.COOKIE, auth.make_session(auth.current_user()), max_age=auth.TTL,
                 httponly=True, samesite='strict', path='/', secure=(request.url.scheme == 'https'))
    return r

@app.post('/api/logout')
def logout(request: Request):
    if auth.valid_session(request.cookies.get(auth.COOKIE, '')): auth.bump_epoch()   # 真的作废令牌,不只是删浏览器那一份
    r = JSONResponse({'ok': True}); r.delete_cookie(auth.COOKIE, path='/'); return r

@app.post('/api/auth/set')
async def auth_set(body: dict, request: Request):
    """设置/修改登录口令:只存 scrypt 哈希。密码为空 = 关闭登录(会在首页挂警告)"""
    user = (str(body.get('user') or 'root').strip() or 'root')[:64]   # 限长:超长用户名配上登录体上限会把自己锁死
    pw = str(body.get('password') or '')
    if pw and len(pw) < auth.MIN_PW: raise HTTPException(400, f'密码至少 {auth.MIN_PW} 位')
    if len(pw) > 128: raise HTTPException(400, '密码最长 128 位')   # 登录接口限了请求体大小,太长的口令设了也登不上
    if auth.enabled():
        cur = auth.current_user()                  # 旧用户名以库里的为准:表单里那个可能已经被改成新的了
        # 🔴这条路也要过同一套闸。漏掉这里的话,拿到一份会话 cookie(共享的/被盗的/同源 XSS)就能用它当口令预言机:
        #    实测 50 次/秒,比登录接口的 0.3 次/秒快 170 倍,还能把线程池全堵在 scrypt 上
        ip = client_ip(request)
        if auth.queue_full(ip) or not auth.take_token(ip): raise HTTPException(429, '请求过于频繁,请稍后再试')
        async with auth.attempt(ip):
            ok_old = await run_in_threadpool(auth.verify, str(body.get('old_user') or cur), str(body.get('old_password') or ''))
            if not ok_old: await auth.penalty(ip)
        if not ok_old: raise HTTPException(401, '旧密码不对')   # 已经开了登录就必须先验旧密码
    db.set_setting('auth_user', user)
    db.set_setting('auth_pass_hash', (await run_in_threadpool(auth.hash_pw, pw)) if pw else '')
    auth.invalidate()      # 中间件读的是 2 秒快照,改完必须立刻作废,否则旧口令还能再用一会儿
    db.log('info', f'登录口令已{"设置" if pw else "关闭"}: 用户 {user}')
    r = JSONResponse({'ok': True, 'enabled': bool(pw)})
    if pw: r.set_cookie(auth.COOKIE, auth.make_session(user), max_age=auth.TTL, httponly=True, samesite='strict', path='/',
                        secure=(request.url.scheme == 'https'))   # 旧会话已作废,给当前浏览器换新的
    return r

@app.get('/')
def index(): return FileResponse(os.path.join(HERE, 'static', 'index.html'), headers={'Cache-Control': 'no-cache'})   # 手机浏览器爱缓存首页,改了样式要能立刻看到
