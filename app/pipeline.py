"""调度:解析 → 批量直链 → 并发 ffprobe → sidecar;喂回批次;指标"""
import threading, time, os, glob, json, statistics, traceback
from concurrent.futures import ThreadPoolExecutor
from config import OUT_DIR, read_cookie
import db, prober
from host import get_host
from api115 import Client115, Breaker, _scrub

def _tail_readable(path, size, n=65536, timeout=60):
    """能不能读到文件最后 64KB:判断「这份文件真的读得到」的独立证据(体积和码率都可能是同源/反推的)。
    ⚠️挂载挂死时这个读线程是拿不回来的,所以超时结论要按挂载点缓存,不能每条都去漏一个线程"""
    root = path.split('/')[1] if path.startswith('/') else path
    hit = _EXISTS_CACHE.get('tail:' + root)
    # ⚠️缓存有效期要大于这里的 timeout:两个都是 60 秒的话,结论是在 t=+60 写的,判据却是 now-ts<60,
    #   等于负缓存刚写好就过期 —— 挂载「stat 通但 read 吊」时这道闸基本等于没有
    if hit and time.time() - hit[0] < max(120, timeout * 2): return False
    box = {}
    def job():
        try:
            with open(path, 'rb') as f:
                f.seek(max(0, size - n))
                box['r'] = len(f.read(n)) >= min(n, size)
        except Exception as e: box['r'] = False
    t = threading.Thread(target=job, daemon=True); t.start(); t.join(timeout)
    if 'r' not in box: _EXISTS_CACHE['tail:' + root] = (time.time(), False); return False   # 超时 = 挂载大概率挂了,一分钟内别再试
    return bool(box['r'])

_EXISTS_CACHE = {}
def _exists(path, timeout=8, ck=None):
    """带超时的存在性检查:CD2/FUSE 挂死时 os.path.exists 会永久阻塞,把槽位卡掉。
    ⚠️超时的那个探测线程是拿不回来的(卡在不可中断的 FUSE 读里),所以超时结论要缓存 60 秒,不能每条都去漏一个线程。
    ck:缓存命名空间。默认按挂载点算(同一个挂载挂了就是全挂了,不必每条都试);
       ⚠️设置页那种「顺手看一眼」的调用必须传自己的 ck —— 它用的超时更短,一旦把 False 写进公共缓存,
         流水线那条真正要紧的 CD2 兜底会在接下来 60 秒里直接跳过,本来能救回来的条目全判成「体积对不上」"""
    root = ck or (path.split('/')[1] if path.startswith('/') else path)
    hit = _EXISTS_CACHE.get(root)
    if hit and time.time() - hit[0] < 60 and hit[1] is False: return False   # 上次超时/不存在 → 一分钟内不再试
    box = {}
    t = threading.Thread(target=lambda: box.setdefault('r', os.path.exists(path)), daemon=True)
    t.start(); t.join(timeout)
    r = box.get('r')
    if r is None: _EXISTS_CACHE[root] = (time.time(), False); return False    # 超时:挂载大概率挂了
    return bool(r)

def THUMB_MAX():
    """截图的重试上限。比媒体信息宽一些:这个计数里还含「等媒体信息落实」的顺延次数,
    ⚠️三个选择器和 thumb_fail 必须用同一个值,否则会出现「计数还没到上限却已经没人捡它」的死格"""
    return int(db.setting('max_retries')) + 5

class Slots:
    """115 下载并发总闸:提取和截图扫尾共用,上限随设置热调(两边各开一套池子会翻倍,默认上限 3 条(115 对并发很敏感))"""
    def __init__(self): self.c = threading.Condition(); self.n = 0; self._lim = (0.0, 3)
    def limit(self):
        ts, v = self._lim
        if time.time() - ts < 1.0: return v                  # 缓存 1 秒:这个值每次等待都要读,不能每次都去拿全局 DB 锁
        try: v = max(1, min(3, int(db.setting('probe_workers'))))   # 🔴上限写死 3:115 对并发很敏感,设置页不该能越过这个上限
        except Exception: v = 3
        self._lim = (time.time(), v); return v
    def acquire(self, timeout=None):
        t0 = time.time()
        while True:
            lim = self.limit()                               # 🔴锁外读:握着 self.c 去读设置会连 release 一起挡在门外
            with self.c:
                if self.n < lim: self.n += 1; return True
                if timeout is not None and time.time() - t0 > timeout: return False
                self.c.wait(0.5)
    def release(self):
        with self.c: self.n = max(0, self.n - 1); self.c.notify()
    @property
    def busy(self):
        with self.c: return self.n

class Pipeline:
    def __init__(self):
        self.client = None; self.stop = False
        self.probe_times = []; self.done_ts = []
        self.state = {'phase': 'idle', 'last_error': None, 'feed': {}}
        self.inflight = {}; self.queued = set(); self.feed_lock = threading.Lock(); self.net = {'mbps': 0.0, 'hist': [], 'total_rx': 0}
        self.slots = Slots(); self.capture_times = []; self.trunc = []; self.trunc_lock = threading.Lock(); self.tree_dropped = {}
        self.last_retry_sweep = time.time()   # ⚠️初值给「现在」:否则每次重启都会立刻把失败条目放回去重试一遍
        self.cookie = {'ok': None, 'user_id': None, 'user_name': None, 'vip': None, 'checked_at': None, 'err': None}
        os.makedirs(OUT_DIR, exist_ok=True)
    # ---------- 生命周期 ----------
    def start(self):
        # 上次进程中断留下的中间态复位
        n1 = db.x("update items set status='pending' where status='probing'")
        # 调小过 max_retries 会造出「还是 pending、但 attempts 已经用满」的行:选择器筛不到它们,
        # 面板却一直把它们算进「待处理」,进度永远差一口气。启动时顺手放回去(和控制页那个按钮一个意思,只是不用人点)
        n0 = db.x("update items set attempts=0 where status='pending' and attempts >= ?", (int(db.setting('max_retries')),))
        n0b = db.x("update items set thumb_attempts=0 where thumb='pending' and thumb_attempts >= ?", (THUMB_MAX(),))   # 截图这条线同理
        if n0 or n0b: db.log('warn', f'启动复位: {n0} 条「待处理但重试次数已满」、{n0b} 条「待截图但次数已满」已放回(多半是把最大重试调小过)')
        db.x("update items set thumb='captured' where thumb='feeding'")
        feeding = [r['id'] for r in db.q("select id from items where status='feeding'")]
        if n1 or feeding: db.log('warn', f'启动复位: probing→pending {n1}, 入库中待核实 {len(feeding)}')
        if feeding: threading.Thread(target=self.reconcile, args=(feeding,), daemon=True, name='reconcile').start()
        try: self.client = Client115(read_cookie())
        except Exception as e: db.log('error', f'cookie 读取失败: {e}')
        self._loops = {'extract': self.extract_loop, 'feed': self.feed_loop, 'metrics': self.metrics_loop,
                       'cookie': self.cookie_loop, 'net': self.net_loop, 'capture': self.capture_loop}
        self._threads = {}
        for n, fn in self._loops.items(): self._spawn(n, fn)
        threading.Thread(target=self.supervisor, daemon=True, name='supervisor').start()

    def _spawn(self, name, fn):
        t = threading.Thread(target=fn, daemon=True, name=name); self._threads[name] = t; t.start()

    def supervisor(self):
        """🔴谁死了就把谁拉起来。
        踩过:磁盘满的时候三个循环线程同时静默退出(兜底 except 里调 db.log,db.log 自己也写库),
        而面板仍然显示「提取中」、日志页再无新行、容器健康检查一路绿 —— 只能靠人察觉。
        这个线程自己要足够笨:只做 is_alive 判断,不碰库、不抛异常。"""
        while not self.stop:
            time.sleep(20)
            for n, fn in (self._loops or {}).items():
                try:
                    t = self._threads.get(n)
                    if t is not None and not t.is_alive():
                        self._spawn(n, fn); db.log('error', f'🔴 {n} 线程死了,已重新拉起(多半是写库失败,查磁盘空间)')
                except Exception: pass
    # ---------- 网速(容器网卡下行 ≈ 115 拉流带宽) ----------
    @staticmethod
    def _rx_bytes():
        """取除 lo 外收包最多的网卡(容器里还有 tunl0 之类的空网卡)"""
        best = None
        try:
            for line in open('/proc/net/dev'):
                if ':' not in line: continue
                name, rest = line.split(':', 1); name = name.strip()
                if name == 'lo': continue
                rx = int(rest.split()[0])
                if best is None or rx > best: best = rx
        except Exception: return None
        return best
    def net_loop(self):
        last = self._rx_bytes(); lt = time.time(); base = last or 0
        while not self.stop:
            time.sleep(2)
            cur = self._rx_bytes()
            if cur is None or last is None: last = cur; continue
            now = time.time(); mbps = (cur - last) * 8 / (now - lt) / 1e6
            self.net['hist'].append((now, round(mbps, 1)))
            self.net['mbps'] = round(sum(v for _, v in self.net['hist'][-5:]) / max(1, len(self.net['hist'][-5:])), 1); self.net['total_rx'] = cur - base
            self.net['hist'] = self.net['hist'][-90:]
            last, lt = cur, now
    # ---------- cookie ----------
    def check_cookie(self, client=None, mutate=True):
        """mutate=False:只返回结果,不动全局的 cookie 展示状态。
        ⚠️校验「用户粘进来的候选 cookie」必须用 False —— 否则粘错一次,面板立刻报「当前 cookie 失效」,
          而且 user_id/vip 还留着旧账号的,拼成一份假状态,会把人引去换掉本来好好的线上 cookie"""
        client = client or self.client
        cur = dict(self.cookie)
        if client is None:
            cur.update(ok=False, err='无 cookie', checked_at=time.time())
            if mutate: self.cookie.update(cur)
            return cur
        try:
            r = client.call(client.c.user_my); d = r.get('data', {}) if isinstance(r, dict) else {}
            cur.update(ok=True, user_id=str(d.get('user_id') or ''), user_name=d.get('user_name'), vip=d.get('vip'), checked_at=time.time(), err=None)
        except Breaker as e:
            # 熔断期间根本没发出请求,这不是「cookie 失效」。照报失效会把人引去 115 重新登录 —— 而那正是 WAF 生气时最不该做的
            cur.update(checked_at=time.time(), err='熔断中,本次未验活: ' + str(e)[:120])
        except Exception as e:
            cur.update(ok=False, err=_scrub(str(e))[:200], checked_at=time.time())
        if mutate: self.cookie.update(cur)
        return dict(cur)
    def replace_cookie(self, cookie):
        from config import COOKIE_FILE
        new = Client115(cookie)
        info = self.check_cookie(new, mutate=False)      # 候选 cookie 的结论不许写进全局状态
        if not info['ok']: raise ValueError('cookie 无效: ' + str(info['err']))
        os.makedirs(os.path.dirname(COOKIE_FILE), exist_ok=True)
        # 🔴原子写:直接 open('w') 写到一半失败(满盘/被 kill)会留下截断文件,而旧 cookie 已经没了。
        #   更阴的是 client 是在写完之后才换的 —— 进程里一切照常,故障要等下次重启才爆,
        #   那时 115 只会回「请重新登录」,而这个状态按设计是永不自愈的
        tmp = COOKIE_FILE + '.tmp'
        fd = os.open(tmp, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as f:
            f.write(cookie.strip() + '\n'); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, COOKIE_FILE)
        os.chmod(COOKIE_FILE, 0o600)
        old = self.client; self.client = new
        if old:
            with old.dir_lock: new.dir_cache = dict(old.dir_cache); new.fpath_cache = dict(old.fpath_cache)   # 拷贝而不是共享(旧 client 还有线程在写)
            with old.tree_lock: new.tree = dict(old.tree)   # ⚠️同理:共享同一个 dict 但各拿各的锁,旧 client 的预取线程会去淘汰新 client 正在用的树
        self.cookie.update(info)
        db.log('info', f"cookie 已更换: 账号 {info['user_id']} {info['user_name']} vip={info['vip']}")
        return info
    def cookie_loop(self):
        time.sleep(5); self.check_cookie()
        while not self.stop:
            time.sleep(3600)
            try:
                info = self.check_cookie()
                if not info['ok']: db.log('error', f"cookie 验活失败: {info['err']}")
            except Exception: pass
    def reconcile(self, ids):
        """上次中断在「入库中」的条目:向 Emby 核实,有流记录→done,否则回到 extracted 等重发"""
        try:
            now = time.time(); done = 0; back = 0
            for c in range(0, len(ids), 500):
                part = ids[c:c+500]; st = get_host().verify(part)
                for i in part:
                    n, has_p = (st.get(str(i)) or [-1, False])[:2]   # verify 现在回 [流数, 有图, 有背景图]
                    # ⚠️带 status='feeding' 守卫:这批核实要跑好几分钟,期间 feed_loop 可能已经把某条重新发出去了,
                    #   裸写会把人家正在飞的状态覆盖掉
                    if n and n > 0:
                        if db.x("update items set status='done', fed_at=?, error=null, updated_at=? where id=? and status='feeding'", (now, now, i)): self._drop_sidecar(i); done += 1
                    elif db.x("update items set status='extracted', updated_at=? where id=? and status='feeding'", (now, i)): back += 1
                    if has_p and db.x("update items set thumb='done' where id=? and thumb in ('pending','captured')", (i,)):
                        db.x('update items set backdrop=3 where id=? and backdrop=1', (i,))   # 不收尾的话「背景图待生成」这个数字永远降不下去
                        try: os.remove(os.path.join(OUT_DIR, f'{i}-thumb.jpg'))   # 和 _feed_once 一样要删,否则暂存目录只涨不落
                        except Exception: pass
            db.log('info', f'入库核实完成: 已在库 {done} 条标完成, 回退重发 {back} 条')
        except Exception as e:
            # ⚠️只回退「本来就要核实的这批」,而且要等当前入库让开:裸写 where status='feeding' 会把正在飞的那批也掀翻,导致同一批被刷两遍
            if not self.feed_lock.acquire(timeout=120):
                # 🔴拿不到锁就什么都别写。原来这里 got 只管 release、回退照样执行,于是会把「正在飞的那一批」
                #    一起掀回 extracted:那批回来时守卫条件全落空,attempts 不增、error 清空,下一轮又原样重发一遍生产库
                db.log('error', f'入库核实失败,但入库正在进行、拿不到锁,这批 {len(ids)} 条暂不回退(看门狗 3 小时后会兜底): {str(e)[:160]}')
                return
            try:
                for c in range(0, len(ids), 900):
                    part = ids[c:c+900]
                    db.x(f"update items set status='extracted', updated_at=? where status='feeding' and id in ({','.join('?'*len(part))})", (time.time(), *part))
            finally: self.feed_lock.release()
            db.log('error', f'入库核实失败,这批 {len(ids)} 条回退重发: {str(e)[:200]}')
    # ---------- 提取 ----------
    def extract_loop(self):
        """resolver:持续把 pending 解析成 (item, url) 塞进队列;worker 线程并发 ffprobe。两阶段重叠,网卡不再空转"""
        import queue
        # 队列深一点:建一棵分类树要十几分钟(115 的锁被建树独占),40 条缓冲 30 秒就抽干,
        # 之后三个槽位一直空转。300 条 ≈ 3~5 分钟缓冲,能吸收掉大部分停摆。
        # ⚠️代价是直链会提前几分钟取(115 直链寿命远大于此),以及熔断/重启时最多 300 条留在 probing —— start() 和 2 小时看门狗都会复位
        self.q = queue.Queue(maxsize=300)
        for i in range(6): threading.Thread(target=self.worker, args=(i,), daemon=True, name=f'probe{i}').start()
        while not self.stop:
            try:
                if db.setting('paused') or self.client is None or self.client.tripped():
                    self.state['phase'] = 'paused' if db.setting('paused') else ('breaker' if self.client and self.client.tripped() else 'no-cookie'); time.sleep(3); continue
                if time.time() < getattr(self, 'cooldown_until', 0):
                    self.state['phase'] = 'cooldown'; time.sleep(10); continue          # 被 115 CDN 限流,歇一会
                batch = db.q("select * from items where status='pending' and attempts < ? order by priority desc, target limit ?", (int(db.setting('max_retries')), int(db.setting('url_batch'))))
                if not batch: self.state['phase'] = 'idle' if self.q.empty() else 'extracting'; time.sleep(5); continue
                self.state['phase'] = 'extracting'
                self.resolve_batch([dict(r) for r in batch])
            except Breaker as e:
                db.log('error', f'熔断中: {e}'); time.sleep(30)
            except Exception as e:
                self.state['last_error'] = str(e)[:300]; db.log('error', 'extract_loop: ' + traceback.format_exc()[-600:]); time.sleep(5)
    def resolve_batch(self, items):
        try: self._resolve_batch(items)
        except Breaker:
            ids = [it['id'] for it in items if it['id'] not in self.queued]
            if ids: db.x(f"update items set status='pending' where status='probing' and id in ({','.join('?'*len(ids))})", ids)
            for i in ids: self.inflight.pop(i, None)
            raise
    def _resolve_batch(self, items):
        ua = db.setting('user_agent'); timeout = int(db.setting('probe_timeout'))
        ids = [it['id'] for it in items]
        db.x(f"update items set status='probing', updated_at=? where id in ({','.join('?'*len(ids))})", (time.time(), *ids))
        http_items = []
        for it in items:
            # 🔴tok:登记凭据。没有它会出这种事 —— probe_one 失败把条目写回 pending、调度器立刻重新选中并重新登记,
            #   而 probe_one 的 finally 这时才执行 pop,把**新登记的那份**删掉 → 后面 `self.inflight[id][...]` 抛 KeyError
            #   逃出 extract_loop,条目永久卡在 probing;更要命的是卡住的 probing 让 has_pending() 恒真,
            #   截图线程和周期性同步从此静默停摆(代理 60 秒复现 11 次)
            it['_tok'] = tok = f"{it['id']}:{time.time():.6f}"
            self.inflight[it['id']] = {'id': it['id'], 'name': os.path.basename(it['target']), 'kind': it['kind'], 'stage': '解析', 'since': time.time(), 'size': it.get('size'), 'tok': tok}
            try:
                pre = (db.setting('strm_115_prefix') or '').rstrip('/')
                if not pre or not it['target'].startswith(pre + '/'):
                    # ⚠️同截图那处:这是配置错,全库都会命中。原来会把条目一条条烧成 failed ——
                    #   实测每秒能烧掉几千条,整库几分钟就全进终态,而且恢复要点两个按钮(还没人提示第二个)。
                    #   现在只跳过 + 限流提醒,填对前缀后自己就接着跑
                    self.note_prefix_mismatch(pre or '(空)', it['target'])
                    self.unmark(it); continue
                rel = it['target'][len(pre):]
                excl = [x.strip().lower() for x in (db.setting('exclude_exts') or '').split(',') if x.strip()]
                if it['ext'] in excl:
                    db.update_item(it['id'], status='skipped', error=f"容器 {it['ext']} 在排除列表(神医同样不提取/不恢复)"); self.unmark(it); continue
                if it['ext'] == '.iso':
                    root = (db.setting('cd2_115_root') or '').rstrip('/')
                    if not root or not os.path.isdir(root):
                        db.update_item(it['id'], status='skipped', error='ISO 需要挂载 115(cd2_115_root)才能探测'); self.unmark(it); continue
                    self.mark(it, stage='排队'); self.queued.add(it['id']); self.q.put((it, root + rel, ua, max(timeout, 600), True)); continue
                fid, pc, size, sha = self.client.resolve(rel)
                if it.get('size_ok'):                       # 我们自己读出来核实过的体积,别被 115 清单的数字覆盖回去
                    db.update_item(it['id'], pickcode=pc, sha1=sha); size = int(it.get('size') or size)
                else:
                    db.update_item(it['id'], pickcode=pc, size=size, sha1=sha)
                it['rel'] = rel; it['pc'] = pc; it['fid'] = fid; it['size'] = size; http_items.append(it)
                self.mark(it, size=size, stage='取直链')
            except Breaker: raise
            except Exception as e: self.fail(it, f'解析: {e}'); self.unmark(it)
        # 预取下一个分类的树(当前分类的树加载完后,趁探测在跑把下一个分类拉好)
        try:
            cur_cat = self.client.category_of(http_items[0]['rel']) if http_items else None
            if cur_cat and cur_cat in self.client.tree:
                pre = (db.setting('strm_115_prefix') or '').rstrip('/')
                nxt = db.q("select target from items where status='pending' and target not like ? order by priority desc, target limit 1", (pre + cur_cat + '/%',))
                if nxt: self.client.prefetch(self.client.category_of(nxt[0]['target'][len(pre):]))
        except Exception: pass
        if not http_items: return
        try: urls = self.client.urls([it['pc'] for it in http_items], ua)
        except Breaker: raise
        except Exception as e:
            for it in http_items: self.fail(it, f'取直链: {e}'); self.inflight.pop(it['id'], None)
            return
        errs = urls.get('__errors__') or {}; umeta = urls.get('__meta__') or {}
        for it in http_items:
            u = urls.get(it['pc']) or urls.get(str(it.get('fid')))
            if not u: self.fail(it, '取直链: ' + errs.get(it['pc'], '直链缺失')); self.inflight.pop(it['id'], None); continue
            m = umeta.get(it['pc']) or umeta.get(str(it.get('fid'))) or {}
            real = str(m.get('name') or ''); want = os.path.basename(it['target'])
            if not real: self.note_noname()      # 115 没回名字 → 这道「解析对不对得上」的闸这次是空的,要让人看得见
            if real and real != want:      # 🔴解析到了别的文件(树过期/同名撞车):立刻拦下,并把这个分类的树作废让它重建
                self.fail(it, f'解析对不上: 115 上这个 pickcode 的文件叫「{real[:60]}」,不是「{want[:60]}」')
                self.inflight.pop(it['id'], None)
                try: self.drop_tree_limited(self.client.category_of(it.get('rel') or ''))
                except Exception: pass
                continue
            try:
                if m.get('size') and int(m['size']) != (it.get('size') or 0) and not it.get('size_ok'):   # 以直链给的体积为准(清单里的偶尔是错的);已核实过的不动
                    it['size'] = int(m['size']); db.update_item(it['id'], size=it['size'])
                if m.get('sha1') and str(m['sha1']) != (it.get('sha1') or ''): db.update_item(it['id'], sha1=str(m['sha1']))
            except Exception: pass
            self.mark(it, stage='排队'); self.queued.add(it['id']); self.q.put((it, u, ua, timeout, False))
    def worker(self, idx):
        while not self.stop:
            if idx >= self.slots.limit(): time.sleep(2); continue   # 并发数可在 UI 热调;用 Slots 缓存过的值,别每 2 秒去抢全局 DB 锁
            try: job = self.q.get(timeout=2)
            except Exception: continue
            try: self.probe_one(*job)
            except Exception:                                # 🔴worker 一死,extract_loop 会永远卡在 q.put 上(队列满 40),phase 还显示「提取中」
                try:
                    it = job[0]; db.log('error', f"探测线程异常 {it.get('id')}: " + traceback.format_exc()[-400:])
                    db.x("update items set status='pending', updated_at=? where id=? and status='probing'", (time.time(), it['id']))
                    self.unmark(it); self.queued.discard(it['id'])
                except Exception: pass
            finally: self.q.task_done()
    def probe_one(self, it, src, ua, timeout, is_iso):
        if db.setting('paused'):
            # ⚠️队列里最多排着几百条,不看这个开关的话点了暂停还会继续打 115 —— 而按暂停的理由常常正是「115 在限流」
            db.x("update items set status='pending', updated_at=? where id=? and status='probing'", (time.time(), it['id']))
            self.unmark(it); self.queued.discard(it['id']); return
        if time.time() < getattr(self, 'cooldown_until', 0):
            # 限流冷却期:队列里排着的也别硬探,放回去等下一轮(否则 40 个排队任务会把 attempts 全烧在限流期)
            db.x("update items set status='pending', updated_at=? where id=? and status='probing'", (time.time(), it['id']))
            self.unmark(it); self.queued.discard(it['id']); return
        self.slots.acquire(); t0 = time.time()
        try:
            self.inflight.setdefault(it['id'], {'id': it['id'], 'name': os.path.basename(it['target']), 'kind': it['kind'], 'since': t0, 'size': it.get('size')})['stage'] = 'ISO 探测' if is_iso else '探测'
            pr = prober.probe_iso(src, timeout) if is_iso else prober.probe_http(src, ua, timeout)
            rel = it.get('rel') or it['target'][len((db.setting('strm_115_prefix') or '').rstrip('/')):]
            want = int(it.get('size') or 0)
            def trunc(p):      # 读到的体积和 115 说的差 >2% = 没读全(CDN 403 限流时 ffprobe 照样 rc=0 吐一份「合法」结构)
                fs = int((p.get('format') or {}).get('size') or 0)
                return bool(want and fs and abs(fs - want) > max(1024, want * 0.02)), fs
            bad, fsize = (False, 0) if is_iso else trunc(pr)
            if not is_iso and not want:
                # 115 那边没给体积 → 「读不全」这道闸这一次是空的(它全靠拿实际读到的和应有的比)。
                # 不因此判失败(有些账号/目录本来就不返回体积),但要让人看得见,别以为一直有保护
                self.note_nosize()
            if bad:
                cd2 = (db.setting('cd2_115_root') or '').rstrip('/') + rel      # 🔴HTTP 被限流 → 换 CD2 本地挂载重读一次(慢 5 倍但读得全)
                try:
                    if cd2 and _exists(cd2, 8):                                 # FUSE 卡住时 os.path.exists 会一直阻塞,套个超时
                        self.inflight.get(it['id'], {})['stage'] = 'CD2 兜底'
                        pr2 = prober.run([prober.FFPROBE] + prober.ARGS + ['-i', cd2], min(timeout, 600))
                        f2 = pr2.get('format') or {}
                        size2 = int(f2.get('size') or 0); dur2 = float(f2.get('duration') or 0)
                        # ⚠️别拿 format 里的数字自证:本地文件的 size 就是 st_size(和 115 清单同源),
                        #    而 bit_rate 在容器没声明时是 ffprobe 用 size*8/duration 反推的 → 「时长×码率≈体积」也恒成立。
                        #    真正独立的证据只有一个:这个文件的尾部读不读得到。读得到 = 整份在手,ffprobe 的数字就可信。
                        consistent = dur2 > 0 and size2 > 0 and _tail_readable(cd2, size2)
                        if consistent:
                            pr = pr2; bad = False; fsize = size2; src = cd2
                            self.cd2_saves = getattr(self, 'cd2_saves', 0) + 1
                            if size2 != want:
                                # 115 清单的数字不准(实测 23.8G vs 19.2G)→ 以读到的为准并打上 size_ok,
                                # 否则下次解析又会被 115 的数字覆盖回去,截图那条路就会永远判它「读不全」
                                db.update_item(it['id'], size=size2, size_ok=1)
                                it['size'] = size2
                                db.log('warn', f"{it['id']} CD2 读到 {size2},115 说 {want},以实际为准")
                        else:
                            db.log('warn', f"{it['id']} CD2 也读不全(时长 {dur2}s,或者读不到文件尾)")
                except Exception as e:
                    db.log('warn', f"{it['id']} CD2 兜底失败: {str(e)[:120]}")
                if bad:
                    # ⭐分清两件事:读到的远小于应有体积(比如 79 字节)= 真被限流,值得冷却;
                    #   读到的体积只是「对不上」(115 清单本来就不准,实测有 23.8G vs 19.2G)= 这一条的问题,别停整条线
                    if fsize < want * 0.5:
                        self.throttled()
                        raise RuntimeError(f'探测结果不完整(115 限流?):读到 {fsize} 字节,115 说 {want} 字节')
                    raise RuntimeError(f'体积对不上:读到 {fsize} 字节,115 说 {want} 字节(CD2 也核不了)')
            side = prober.sidecar(pr, rel, is_iso)
            if not side[0]['MediaSourceInfo'].get('RunTimeTicks') or not side[0]['MediaSourceInfo'].get('Size'): raise RuntimeError('探测结果缺时长或体积(神医会判为异常媒体信息)')
            if not is_iso and want and fsize and fsize != want:        # 差一点点:115 清单体积偶尔不准,以实际读到的为准(与神医经 CD2 读到的一致)
                db.log('warn', f"{it['id']} 115 体积 {want} ≠ 实际 {fsize},以实际为准: {os.path.basename(it['target'])[:60]}")
            out = os.path.join(OUT_DIR, f"{it['id']}.json")
            with open(out, 'w', encoding='utf-8') as f: json.dump(side, f, ensure_ascii=False)
            ms = int((time.time() - t0) * 1000); self.probe_times.append(ms); self.probe_times = self.probe_times[-500:]
            # ⚠️attempts 清零:这个计数接下来要被「入库复核」阶段当额度用,探测阶段用掉几次不该算到那边
            db.x("update items set status='extracted', attempts=0, probe_ms=?, streams=?, chapters=?, extracted_at=?, sidecar=?, error=null, updated_at=? where id=? and status='probing'",
                 (ms, len(side[0]['MediaSourceInfo']['MediaStreams']), len(side[0]['Chapters']), time.time(), out, time.time(), it['id']))
            if db.setting('capture_mode') == 'with_probe' and (it.get('thumb') == 'pending'):
                if is_iso: db.x("update items set thumb='failed', thumb_err='ISO 暂不支持截图' where id=?", (it['id'],))
                else: self.capture_one(it, src, pr, ua)
        except Exception as e:
            msg = str(e)
            if is_iso and ('bd_open() failed' in msg or 'index.bdmv' in msg):   # 不是蓝光结构(多半是 DVD 镜像),libbluray 打不开 → 跳过而非失败
                db.x("update items set status='skipped', error=?, updated_at=? where id=? and status='probing'", ('ISO 不是蓝光结构(DVD 镜像不支持)', time.time(), it['id']))
            elif is_iso and 'AACS' in msg:                                          # 加密蓝光(AACS),无解密库 → 跳过
                db.x("update items set status='skipped', error=?, updated_at=? where id=? and status='probing'", ('ISO 为 AACS 加密蓝光,不支持', time.time(), it['id']))
            else: self.fail(it, f'探测: {msg[:200]}')
        finally: self.slots.release(); self.unmark(it); self.queued.discard(it['id'])
    def capture_one(self, it, src, pr, ua):
        self.inflight.setdefault(it['id'], {'id': it['id'], 'name': os.path.basename(it['target']), 'kind': it['kind'], 'since': time.time(), 'size': it.get('size')})['stage'] = '截图'
        out = os.path.join(OUT_DIR, f"{it['id']}-thumb.jpg"); t0 = time.time()
        try:
            prober.capture(src, pr, out, ua=ua, position_pct=float(db.setting('capture_position')), tonemap=bool(db.setting('capture_hdr_tonemap')),
                           quality=int(db.setting('capture_quality')), timeout=int(db.setting('probe_timeout')),
                           pick_best=bool(db.setting('capture_pick_best')), avoid_dark=bool(db.setting('capture_avoid_dark')))
            ms = int((time.time() - t0) * 1000); self.capture_times = (getattr(self, 'capture_times', []) + [ms])[-500:]
            db.x("update items set thumb='captured', thumb_path=?, thumb_err=null, thumb_ms=? where id=?", (out, ms, it['id']))
        except Exception as e:
            self.thumb_fail(it['id'], str(e)[:200])          # 走计数:一次抖动不至于永久没图
    def mark(self, it, **kw):
        """改在途信息:只改「还是我登记的那一份」。拿不到就说明这条已被别人接手,安静走开"""
        d = self.inflight.get(it['id'])
        if d is not None and (it.get('_tok') is None or d.get('tok') == it.get('_tok')): d.update(kw)

    def unmark(self, it):
        """撤销登记:只撤自己那份,别把后来者的登记删掉"""
        d = self.inflight.get(it['id'])
        if d is not None and (it.get('_tok') is None or d.get('tok') == it.get('_tok')): self.inflight.pop(it['id'], None)

    def note_prefix_mismatch(self, pre, strm):
        """路径前缀对不上 —— 配置问题,全库都会命中,所以只提醒不判死"""
        with self.trunc_lock:
            self.pfx_bad = getattr(self, 'pfx_bad', 0) + 1
            n = self.pfx_bad
        if n in (1, 10, 100) or n % 1000 == 0:
            db.log('error', f'🔴 路径前缀对不上「{pre}」,已跳过 {n} 条(不计重试次数)—— '
                            f'去设置页把前缀填对,填对后会自己接着跑(例:{strm[:60]})')

    def note_nosize(self):
        """115 没给应有体积 → 「读不全(限流)」的校验这一次形同虚设。同 note_noname,必须可见"""
        with self.trunc_lock:
            self.nosize = getattr(self, 'nosize', 0) + 1
            n = self.nosize
        if n in (1, 10, 100) or n % 1000 == 0:
            db.log('warn', f'115 未提供文件体积,「读不全」校验已跳过 {n} 次(限流时可能拿到不完整的探测结果)')

    def note_noname(self):
        """115 直链没带文件名 → 「解析到的是不是同一个文件」这道闸这一次形同虚设。
        ⚠️它是防「目录树过期/同名撞车把别人的画面当封面传进 Emby」的唯一一道闸,
          一旦上游库换了返回结构,它会安静地失效 —— 所以必须有计数和日志,不能无声无息"""
        with self.trunc_lock:
            self.noname = getattr(self, 'noname', 0) + 1
            n = self.noname
        if n in (1, 10, 100) or n % 1000 == 0:
            db.log('warn', f'115 直链未返回文件名,「解析对不上」校验已跳过 {n} 次(目录树过期时无法拦截)')

    def thumb_fail(self, item_id, err, retry=True):
        """截图失败:可重试的攒次数(满 3 次才判死),判死时把残留 jpg 删掉。
        ⚠️脱敏放在这里,不放在调用方:错误文本进库就会显示在 UI 和导出的 CSV 里,而 115 的异常里带着签名直链。
          十几个调用点各自记得脱敏是靠不住的,写入口只有这一个"""
        err = _scrub(err)
        r = db.q('select thumb_attempts from items where id=?', (item_id,))
        att = int((r[0]['thumb_attempts'] if r else 0) or 0) + 1
        dead = (not retry) or att >= THUMB_MAX()
        # ⚠️updated_at 必须一起刷:选择器里那道「上一轮标了错的压 30 分钟再来」是拿 updated_at 比的,
        #   不刷的话积压的行(updated_at 本来就很旧)会在 15 秒后被原样重新挑中,几分钟就把重试次数烧光
        db.x("update items set thumb=?, thumb_attempts=?, thumb_err=?, updated_at=? where id=?",
             ('failed' if dead else 'pending', att, err, time.time(), item_id))
        if dead:
            # 背景图是拿截图那张去传的,截图判死了它也就没指望了。
            # ⚠️不改的话这条会永远挂在「背景图待生成」里:没人会再给它截图,计数也永远清不掉(线上抓到过 1 条)
            db.x('update items set backdrop=3 where id=? and backdrop=1', (item_id,))
            try: os.remove(os.path.join(OUT_DIR, f'{item_id}-thumb.jpg'))
            except Exception: pass
    def fail(self, it, err):
        err = _scrub(err)      # 同上:入库的错误文本统一在这里脱敏
        att = int(it.get('attempts') or 0) + 1
        status = 'failed' if att >= int(db.setting('max_retries')) else 'pending'
        db.x("update items set status=?, attempts=?, error=?, updated_at=? where id=? and status='probing'", (status, att, err, time.time(), it['id']))
        if status == 'failed':
            db.log('warn', f"失败 {it['id']} {os.path.basename(it['target'])[:60]}: {err}")
            db.x("update items set thumb='failed', thumb_err='媒体信息提取失败,不截图' where id=? and thumb='pending'", (it['id'],))   # 否则永远挂在「待截」里没人管
            db.x('update items set backdrop=3 where id=? and backdrop=1', (it['id'],))   # 同理,背景图也别永远挂在「待生成」
    def throttled(self):
        """记一次「读不全」;5 分钟内超过 8 次就整体冷却 5 分钟(115 CDN 在限流,继续跑只会制造垃圾数据)"""
        with self.trunc_lock:                                  # 三个线程都会调,读改写要加锁
            now = time.time()
            self.trunc = [t for t in self.trunc if now - t < 300] + [now]
            if len(self.trunc) >= 8 and now >= getattr(self, 'cooldown_until', 0):
                # 逐次加码:先歇 1 分钟试试,还在限流再 3 分钟、5 分钟。一上来就停 5 分钟太贵了
                self.cool_step = min(getattr(self, 'cool_step', 0) + 1, 3)
                secs = (0, 60, 180, 300)[self.cool_step]
                self.cooldown_until = now + secs; self.trunc = []
                db.log('error', f'🔴 5 分钟内 8 次探测读不全(115 CDN 限流),暂停提取 {secs} 秒')
            elif now - getattr(self, 'cool_reset_at', 0) > 3600:
                self.cool_reset_at = now; self.cool_step = 0   # 一小时没再触发就把加码清零
    def drop_tree_limited(self, cat):
        """作废分类树:同一个分类 30 分钟内最多一次。重建一个大分类要几十次 115 调用,名字对不上如果不是「树旧了」引起的,会变成反复重建"""
        if not cat: return
        now = time.time()
        if now - self.tree_dropped.get(cat, 0) < 1800: return
        self.tree_dropped[cat] = now; self.client.drop_tree(cat)
    def has_pending(self):
        """还有没有排得上队的待处理(attempts 用满的不算,否则闸门永远关着)。⭐用 limit 1 而不是 count:同样的条件 count 要 116ms 且占着全局 DB 锁,这查询每 5 秒跑一次"""
        return bool(db.q("select 1 from items where (status='pending' and attempts < ?) or status='probing' limit 1", (int(db.setting('max_retries')),)))
    def capture_loop(self):
        """媒体信息阶段已过、只缺封面的条目(after 模式的全部;with_probe 模式下媒体信息早就有的那些)重新取直链只截图。待处理清空后才跑,媒体信息优先"""
        while not self.stop:
            try:
                if db.setting('capture_mode') == 'off' or db.setting('paused') or self.client is None or self.client.tripped(): time.sleep(5); continue
                if time.time() < getattr(self, 'cooldown_until', 0): time.sleep(15); continue      # 被 115 限流时,截图这条路也要停(冷却可能就是它自己触发的)
                if self.has_pending(): time.sleep(10); continue   # 媒体信息优先(只算还排得上队的:attempts 用完的不算)
                excl = [x.strip().lower() for x in (db.setting('exclude_exts') or '').split(',') if x.strip()] + ['.iso']   # 神医的截图排除同样是 ts/m2ts;ISO 暂不截
                db.x(f"update items set thumb='failed', thumb_err='容器在排除列表,不截图' where thumb='pending' and ext in ({','.join('?'*len(excl))})", excl)
                batch = db.q("select * from items where thumb='pending' and thumb_attempts < ? and status in ('done','skipped','extracted','feeding','failed')"
                             " and (thumb_err is null or updated_at < ?) order by target limit ?",
                             (THUMB_MAX(), time.time() - 1800, int(db.setting('url_batch'))))   # 上一轮标了错的,压 30 分钟再来
                if not batch: time.sleep(15); continue
                ua = db.setting('user_agent'); pre = (db.setting('strm_115_prefix') or '').rstrip('/'); ok = []
                for r in batch:
                    it = dict(r)
                    try:
                        if not pre or not it['target'].startswith(pre + '/'): raise ValueError('strm 内容不以「115 路径前缀」开头')
                        rel = it['target'][len(pre):]; fid, pc, size, sha = self.client.resolve(rel)
                        it['pc'] = pc; it['fid'] = fid
                        if not it.get('size_ok'): it['size'] = size      # 已核实过的保留库里的值(115 清单不准的那些)
                        ok.append(it)
                    except Breaker: raise
                    except Exception as e: self.thumb_fail(it['id'], '解析: ' + str(e)[:150])
                try: urls = self.client.urls([it['pc'] for it in ok], ua) if ok else {}
                except Breaker: raise
                except Exception as e:
                    for it in ok: self.thumb_fail(it['id'], '取直链: ' + str(e)[:150])
                    continue
                errs = urls.get('__errors__') or {}; umeta2 = urls.get('__meta__') or {}
                def job(it):
                    u = urls.get(it['pc']) or urls.get(str(it.get('fid')))
                    if not u: self.thumb_fail(it['id'], '取直链: ' + errs.get(it['pc'], '直链缺失')); return
                    m = umeta2.get(it['pc']) or umeta2.get(str(it.get('fid'))) or {}
                    real = str(m.get('name') or ''); want = os.path.basename(it['target'])
                    if not real: self.note_noname()
                    # 已核实过体积的条目:比对基准换成「我们自己从 CD2 读出来的那个数」(库里存的),不是 115 报的。
                    # 拿 115 的数字比必然对不上,而这条分支不扣重试次数 → 会永远重来并把限流冷却刷出来。
                    # 🔴但不能把比对整个关掉(上一版就是直接置 0):关掉之后 115 一限流,这条就拿着假时长去截图,
                    #   截出来的黑场/片头会被当成封面传进生产 Emby —— 这道闸本来就是为这个而存在的
                    want_sz = int(it.get('size') or 0) if it.get('size_ok') else int(m.get('size') or it.get('size') or 0)
                    if real and real != want:        # 和提取那条路一样:名字对不上就别截,更别把别人的画面当封面传上去
                        self.thumb_fail(it['id'], f'解析对不上: 115 上是「{real[:50]}」', retry=False)
                        try: self.drop_tree_limited(self.client.category_of(it['target'][len(pre):]))
                        except Exception: pass
                        return
                    self.slots.acquire()
                    try:
                        pr = prober.probe_http(u, ua, int(db.setting('probe_timeout')))
                        got = int((pr.get('format') or {}).get('size') or 0)
                        if want_sz and got and abs(got - want_sz) > max(1024, want_sz * 0.02):
                            # 截图这条路也要防「115 限流读不全」:时长是错的会截到片头/黑场,甚至当成封面传进 Emby。
                            # ⚠️这是限流而不是这条目的错:计入冷却、不扣它的重试次数(否则一次限流就把一批封面判死)
                            self.throttled()
                            # 不扣重试次数,但要压 30 分钟再来(否则每轮都重新挑到同一批,既刷冷却又白烧直链)
                            db.x("update items set thumb_err=?, updated_at=? where id=?",
                                 (f'读不全(115 限流?):{got} ≠ {want_sz},稍后再截', time.time(), it['id']))
                            return
                        self.capture_one(it, u, pr, ua)
                    except Exception as e: self.thumb_fail(it['id'], str(e)[:200])
                    finally: self.slots.release(); self.inflight.pop(it['id'], None); self.queued.discard(it['id'])
                with ThreadPoolExecutor(max_workers=max(1, int(db.setting('probe_workers')))) as ex:
                    list(ex.map(job, ok))
            except Breaker as e: db.log('error', f'熔断中(截图): {e}'); time.sleep(30)
            except Exception as e: db.log('error', 'capture_loop: ' + traceback.format_exc()[-500:]); time.sleep(10)
    # ---------- 喂回 ----------
    def feed_loop(self):
        last = time.time()
        while not self.stop:
            try:
                fb = int(db.setting('feed_batch'))
                n = db.q("select count(*) n from items where status='extracted'")[0]['n']
                tn = db.q("select count(*) n from items where thumb='captured' and status in ('done','skipped','failed')")[0]['n']   # 媒体信息早就有(或提取失败)、只等着把图送过去的;口径必须和 _feed_once 选的一致
                # 真正没活了才算空闲:没待处理、没在截的、队列空、并发闸空(重启瞬间 phase 是 idle,不能拿它判)
                cap_on = db.setting('capture_mode') != 'off'
                # ⚠️这里的条件必须和 capture_loop 的选择器一模一样。少了 status/退避那两段的话,
                #   一条「capture_loop 根本捡不起来」的行就能让 idle 永远为假,尾巴要干等一个 feed_interval(默认半小时)
                idle = (not self.has_pending()) and (not hasattr(self, 'q') or self.q.empty()) and self.slots.busy == 0 \
                       and not (cap_on and db.q("select 1 from items where thumb='pending' and thumb_attempts < ?"
                                                " and status in ('done','skipped','extracted','feeding','failed')"
                                                " and (thumb_err is null or updated_at < ?) limit 1", (THUMB_MAX(), time.time() - 1800)))
                # 攒够一批 / 彻底空闲把尾巴发掉 / 兜底间隔;只缺图的也算数,否则提取一直满速时它们永远挤不进批次
                if (n + tn) and (n + tn >= fb or idle or time.time() - last > float(db.setting('feed_interval'))) and not db.setting('paused'):
                    if self.feed_once() is not False: last = time.time()
                time.sleep(5)
            except Exception as e:
                self.state['last_error'] = 'feed: ' + str(e)[:300]; db.log('error', 'feed_loop: ' + traceback.format_exc()[-600:]); time.sleep(30)
    def feed_once(self):
        if not self.feed_lock.acquire(blocking=False): db.log('warn', '入库正在进行,忽略重复触发'); return False
        try: self._feed_once()
        except Exception as e: db.log('error', f'入库异常: {str(e)[:300]}'); raise
        finally:
            # ⚠️阶段标记必须在这儿收尾:原来只在 _feed_once 最后一行置 idle,中途抛异常就永远停在「feeding N」,
            #   而「立即入库」按钮是看这个标记变灰的 —— 一次异常就能让它再也点不动
            self.state['phase_feed'] = 'idle'
            self.feed_lock.release()
    def _feed_once(self):
        fb = max(1, int(db.setting('feed_batch')))
        tn = db.q("select count(*) n from items where thumb='captured' and status in ('done','skipped','failed')")[0]['n']
        reserve = min(max(fb // 10, 1), tn, max(fb - 1, 0))          # 每批留一成给「只缺图」的,但至少给媒体信息留 1 席
        rows = db.q("select id,strm,sidecar,thumb,thumb_path,kind,backdrop,attempts from items where status='extracted' order by extracted_at limit ?", (fb - reserve,))
        pre = db.setting('emby_strm_prefix').rstrip('/'); bd_on = bool(db.setting('capture_backdrop'))
        files = {}; ids = []; thumb_ids = []; backdrop_ids = []; thumb_files = {}; img_bytes = [0]
        IMG_CAP = 120 * 1024 * 1024                                  # 一批图片总量封顶:ssh 模式下 files 字典 + tar 缓冲 + getvalue 拷贝 ≈ 3 倍峰值,300M 的包会吃掉 900M 内存
        def add_thumb(r):
            try:
                sz = os.path.getsize(r['thumb_path'])               # 先看大小再读:超了就下批再送,别让最后一张把封顶撑破
                if img_bytes[0] and img_bytes[0] + sz > IMG_CAP: return False
                data = open(r['thumb_path'], 'rb').read(); img_bytes[0] += len(data)
                if r['kind'] == 'Movie':                                   # 电影海报经 Emby API 上传(Emby 自己决定存哪),缺背景图的同一张再传一次当背景图
                    files[f"img/{r['id']}.jpg"] = data
                    if bd_on and int(r['backdrop'] or 0) == 1: backdrop_ids.append(r['id'])
                else:
                    # 🔴前缀不匹配就别硬切:改过 emby_strm_prefix 之后硬切会把 /strm/MOVIE/x.strm 切成 /x,
                    #    图就落到媒体库**根目录**去了(媒体信息 json 不用这个前缀,所以表现是「媒体信息全对、封面静默全废」,
                    #    最后还把死因写成「Emby 仍未认下这张图」)
                    if not r['strm'].startswith(pre + '/'):
                        # ⚠️这是配置错,不是这一条的错:全库都会命中,判死就等于一口气把十几万张缩略图永久毁掉
                        #   (A1-b 那条「前缀填错 5874 条/秒烧成 failed」就是这么来的)。只跳过 + 限流打一条日志
                        self.note_prefix_mismatch(pre, r['strm'])
                        return False
                    rel = 'strm' + r['strm'][len(pre):][:-len('.strm')] + '-thumb.jpg'   # 剧集:-thumb.jpg 放 strm 旁边,Refresh 时 Emby 自己认
                    files[rel] = data; thumb_files[r['id']] = rel               # 落盘前远端还要再确认一次「确实还缺封面」
                thumb_ids.append(r['id']); return True
            except Exception as e: self.thumb_fail(r['id'], f'读截图: {str(e)[:150]}'); return False
        for r in rows:
            try: files['mediainfo' + r['strm'][:-len('.strm')] + '-mediainfo.json'] = open(r['sidecar'], encoding='utf-8').read(); ids.append(r['id'])
            except Exception as e:
                # ⚠️要计次、要能回到 pending 重新探测。原来是直接写 failed 且 attempts=0 ——
                #   「暂存目录被删」这种错既不进每日重试、也不进重核,连「失败重试」按钮都修不好它
                #   (重试 → 重新探测 → 写 sidecar 又 ENOENT);所以还要顺手把目录补回来
                try: os.makedirs(OUT_DIR, exist_ok=True)
                except Exception: pass
                att = int((r['attempts'] if 'attempts' in r.keys() else 0) or 0) + 1
                mr = int(db.setting('max_retries'))
                db.x("update items set status=?, attempts=?, error=?, sidecar=null, updated_at=? where id=? and status='extracted'",
                     ('failed' if att >= mr else 'pending', att, _scrub(f'读 sidecar: {e}')[:200], time.time(), r['id']))
                continue
            if r['thumb'] == 'captured' and r['thumb_path']: add_thumb(r)
        # 只截了图、媒体信息早就有的(after 模式)也一起送
        thumb_only_ids = []
        if len(ids) < fb:
            for r in db.q("select id,strm,thumb,thumb_path,kind,backdrop from items where thumb='captured' and status in ('done','skipped','failed') order by updated_at limit ?", (fb - len(ids),)):
                if add_thumb(r): ids.append(r['id']); thumb_only_ids.append(r['id'])
        if not ids: return
        # ⚠️必须同时刷新 updated_at:批次是按 extracted_at 从最老的开始取的,不刷的话这些行一进 feeding
        #   就已经「3 小时没动静」,卡死看门狗当场就认为它们超时了
        nowt = time.time()
        db.x(f"update items set status='feeding', updated_at=? where id in ({','.join('?'*len(ids))}) and status='extracted'", (nowt, *ids))
        db.x(f"update items set thumb='feeding', updated_at=? where id in ({','.join('?'*len(thumb_ids))}) and thumb='captured'", (nowt, *thumb_ids)) if thumb_ids else None
        # 🔴认一下这台 Emby 还是不是原来那台。指错服务器的代价是不可逆的:
        #    对方碰巧有同 id 且有流记录的条目,会被我们写成 skipped「Emby 已有媒体信息」——
        #    而 skipped **没有任何批量回收路径**,只能一条条手点;另外几条则真的在别人的服务器上执行了 Refresh
        try:
            sid = str((get_host().emby_info() or {}).get('server_id') or '')
            known = str(db.setting('_emby_server_id') or '')
            if sid and not known: db.set_setting('_emby_server_id', sid)
            elif sid and known and sid != known:
                db.x(f"update items set status='extracted' where id in ({','.join('?'*len(ids))}) and status='feeding'", ids)
                if thumb_ids: db.x(f"update items set thumb='captured' where id in ({','.join('?'*len(thumb_ids))}) and thumb='feeding'", thumb_ids)
                db.set_setting('paused', 1)
                db.log('error', f'🔴 Emby 服务器变了(原 {known[:12]} → 现 {sid[:12]}),已暂停并整批退回。'
                                f'确认这是你要的那台之后,把设置里的 _emby_server_id 清掉再恢复运行')
                return
        except Exception as e: db.log('warn', f'核对 Emby 身份失败(继续入库): {str(e)[:120]}')
        batch_id = time.strftime('%Y%m%d-%H%M%S'); t0 = time.time()
        self.state['phase_feed'] = f'feeding {len(ids)}'
        try:
            res = get_host().ingest(batch_id, files, ids, float(db.setting('refresh_interval')), float(db.setting('latency_threshold')), thumb_ids, backdrop_ids, thumb_files)
        except Exception as e:
            db.x(f"update items set status='extracted' where id in ({','.join('?'*len(ids))}) and status='feeding'", ids)   # 回退,下次再喂
            if thumb_ids: db.x(f"update items set thumb='captured' where id in ({','.join('?'*len(thumb_ids))}) and thumb='feeding'", thumb_ids)
            raise
        if res.get('fatal'): db.log('error', '入库中止: ' + str(res['fatal'])[:200])   # 远端整批退回(比如媒体信息目录只读)
        now = time.time(); fed = set(ids) - set(thumb_only_ids)
        wf = res.get('write_failed') or {}
        if wf:
            rel2id = {v: k for k, v in list(thumb_files.items())}
            id_by_json = {('mediainfo' + r['strm'][:-len('.strm')] + '-mediainfo.json'): r['id'] for r in rows}
            for rel, err in wf.items():
                i = id_by_json.get(rel)
                if i is not None and i in fed: db.update_item(i, status='failed', error='写入媒体信息文件失败: ' + str(err)[:150]); fed.discard(i)
                elif rel in rel2id: self.thumb_fail(rel2id[rel], '写入截图失败: ' + str(err)[:150], retry=False)
            db.log('error', f'{len(wf)} 个文件在 Emby 主机写不进去(已单独判失败,不影响本批其它条目): ' + str(list(wf.items())[:2])[:200])
        for i in res.get('done', []):
            if i in fed: db.update_item(i, status='done', fed_at=now, error=None); self._drop_sidecar(i)
        for i in res.get('skipped', []):
            if i in fed: db.update_item(i, status='skipped', fed_at=now, error='Emby 已有媒体信息'); self._drop_sidecar(i)
        for i, err in (res.get('failed') or {}).items():
            i = int(i)
            if i in fed: db.update_item(i, status='failed', error='喂回: ' + str(err))
        back = [int(i) for i in res.get('unverified', []) if int(i) in fed]
        if back:
            # 🔴整批(或几乎整批)都没核实上 = Emby 那边在忙(扫描媒体库/刷演员这类计划任务一跑就是几小时),
            #    不是这些条目自己有问题 → 这种情况不扣次数,只回退重发。
            #    不加这条判断的代价是实打实的:线上一次「扫描媒体库 + 刷新中文演员」同时跑,
            #    连着几批 2000 条全部 unverified,三轮就把 500 条打成 failed —— 而事后抽查 60/60 其实都在 Emby 里
            systemic = len(back) >= max(50, 0.9 * len(fed))
            mr = int(db.setting('max_retries'))
            for c in range(0, len(back), 900):
                part = back[c:c+900]
                if systemic:
                    db.x(f"update items set status='extracted', error='Emby 正忙,刷新暂未生效,待重发' where id in ({','.join('?'*len(part))}) and status='feeding'", part)
                else:
                    db.x(f"update items set attempts=attempts+1, status=case when attempts+1>=? then 'failed' else 'extracted' end, error=case when attempts+1>=? then '喂回: 多次刷新后 Emby 仍无流记录(可能被插件判为异常媒体信息)' else '刷新后暂未见流记录,待重发' end where id in ({','.join('?'*len(part))}) and status='feeding'", (mr, mr, *part))
            if systemic: db.log('warn', f"{len(back)}/{len(fed)} 条刷新后都没见流记录,判定是 Emby 那边忙(不计次数),整批下次重发")
            else: db.log('warn', f"{len(back)} 条刷新后暂未见流记录,回退下次重发(超过 {mr} 次才判失败)")
        for i in res.get('thumb_done', []):
            db.x("update items set thumb='done', thumb_err=null, thumb_attempts=0 where id=?", (i,))
            # ⚠️截图这条线到此为止(jpg 马上就删了),背景图还挂在「待生成」的话就永远没人管了。
            #   这是线上那 1 条 backdrop=1/thumb=done 的真正来路:预检那一下查 Emby 超时 → 跳过上传,
            #   而 Emby 自己扫到了封面 → 这里判 thumb 完成,背景图却从没传过
            db.x('update items set backdrop=3 where id=? and backdrop=1', (i,))
            try: os.remove(os.path.join(OUT_DIR, f'{i}-thumb.jpg'))
            except Exception: pass
        terr = {int(k): v for k, v in (res.get('thumb_errors') or {}).items() if str(k).lstrip('-').isdigit()}
        for i in res.get('thumb_failed', []): self.thumb_fail(int(i), terr.get(int(i), '刷新后 Emby 仍无 Primary 图'))
        for i, why in (res.get('thumb_skipped') or {}).items():   # 远端发现条目已经有封面了 → 不覆盖,直接收工
            db.x("update items set thumb='done', thumb_err=? where id=?", (str(why)[:100], int(i)))
            db.x('update items set backdrop=3 where id=? and backdrop=1', (int(i),))   # 同上:图不会再有了,背景图别一直挂着
            try: os.remove(os.path.join(OUT_DIR, f'{int(i)}-thumb.jpg'))
            except Exception: pass
        for i in res.get('thumb_deferred', []):    # 媒体信息本身还没落实 → 下批连图一起重发;仍要计数,否则永远落不了地的条目会无限重发
            db.x("update items set thumb='captured', thumb_attempts=thumb_attempts+1 where id=? and thumb='feeding'", (int(i),))
        for i in res.get('thumb_unverified', []):
            msg = terr.get(int(i))
            db.x("update items set thumb='captured', thumb_attempts=thumb_attempts+1, thumb_err=coalesce(?, thumb_err) where id=? and thumb='feeding'", (msg, int(i)))
        db.x("update items set thumb='captured' where thumb='feeding'")   # 兜底:这批里任何没拿到结论的截图都回到可重发
        for r in db.q("select id from items where thumb='captured' and thumb_attempts >= ?", (THUMB_MAX(),)):   # 顺延也计数了,阈值相应放宽
            self.thumb_fail(r['id'], '多次重发后 Emby 仍未认下这张图', retry=False)   # 退场时把暂存 jpg 一起删掉
        if (res.get('thumb_errors') or {}).get('*'): db.log('warn', '本批传图中止: ' + str(res['thumb_errors']['*']))
        for i in res.get('backdrop_done', []): db.x("update items set backdrop=2 where id=?", (i,))
        for i, err in (res.get('backdrop_failed') or {}).items(): db.x("update items set backdrop=3, thumb_err=coalesce(thumb_err,'')||? where id=?", (' 背景图: ' + str(err)[:100], int(i)))
        for l in res.get('lat', []): db.metric('emby_latency', l)
        self.done_ts = (self.done_ts + [now] * len(res.get('done', [])))[-5000:]   # 有界:原来是只增不减,几十万条要攒几十兆
        self.state['feed'] = {'batch': batch_id, 'done': len(res.get('done', [])), 'skipped': len(res.get('skipped', [])),
                              'failed': len(res.get('failed') or {}), 'seconds': res.get('seconds'), 'slowdowns': res.get('slowdowns'), 'at': now}
        db.log('info', f"喂回 {batch_id}: done {len(res.get('done',[]))} skipped {len(res.get('skipped',[]))} failed {len(res.get('failed') or {})}"
                       + (f" 截图 {len(res.get('thumb_done',[]))}/{len(thumb_ids)}" if thumb_ids else '') + (f" 背景图 {len(res.get('backdrop_done',[]))}/{len(backdrop_ids)}" if backdrop_ids else '')
                       + f" 用时 {res.get('seconds')}s"
                       + (f" 延迟 {(res.get('lat') or [0])[-1]:.2f}s" if res.get('lat') else ''))   # 原来是直接打整个列表,日志里会出现 [0.27906153560307834] 这种一长串
        self.state['phase_feed'] = 'idle'
    def reverify_failed(self):
        """把「喂回后仍无流记录」的失败条目重新向 Emby 核实:有流→done,没流→回到 extracted 重发"""
        ids = [r['id'] for r in db.q("select id from items where status='failed' and error like '喂回:%'")]
        if not ids: db.log('info', '没有需要重核的入库失败条目'); return
        done = back = unknown = 0; now = time.time()
        try:
            for c in range(0, len(ids), 500):
                part = ids[c:c+500]; st = get_host().verify(part)
                for i in part:
                    n, has_p = (st.get(str(i)) or [-1, False])[:2]   # verify 现在回 [流数, 有图, 有背景图]
                    if n is None or n < 0: unknown += 1              # 查不出来(Emby 连不上/超时)—— 不能当成「核实过了」
                    # 带 status='failed' 守卫:核实要跑一阵,期间人可能点了「失败重试」把它放回去了,别把人家的新状态覆盖掉
                    if n and n > 0:
                        if db.x("update items set status='done', fed_at=?, error=null, updated_at=? where id=? and status='failed'", (now, now, i)): self._drop_sidecar(i); done += 1
                    elif n == 0 and os.path.exists(os.path.join(OUT_DIR, f'{i}.json')):
                        if db.x("update items set status='extracted', attempts=0, error='重核后仍无流,待重发', updated_at=? where id=? and status='failed'", (now, i)): back += 1
            if unknown >= max(5, 0.9 * len(ids)):
                # 🔴几乎全查不出来 = Emby 那边不可达,不是「这些条目都没入库」。报「完成」会让人以为核实过了
                db.log('error', f'入库失败重核:{unknown}/{len(ids)} 条查不到状态(Emby 不可达?),本次没有得出任何结论')
            else:
                db.log('info', f'入库失败重核完成: 实际已入库 {done} 条改为完成, 回退重发 {back} 条, 共 {len(ids)}'
                               + (f',其中 {unknown} 条查不到状态' if unknown else ''))
        except Exception as e: db.log('error', f'入库失败重核异常: {str(e)[:200]}')
    def _drop_sidecar(self, i):
        """入库确认后删暂存 json(正本已在媒体信息目录),顺带清掉列里的路径"""
        try: os.remove(os.path.join(OUT_DIR, f'{i}.json'))
        except Exception: pass
        db.x('update items set sidecar=null where id=?', (i,))
    # ---------- 指标 ----------
    def metrics_loop(self):
        last_prune = 0
        if not db.setting('_last_sync_at'): db.set_setting('_last_sync_at', time.time())   # 没有基线就以「现在」起算,别让每次重启都立刻同步一次
        while not self.stop:
            time.sleep(30)
            try:
                # 卡在 probing 超过 2 小时的(worker 崩了/进程被杀没来得及复位)→ 回到 pending
                # ⚠️先清掉在途表里躺太久的登记:看门狗靠「不在 inflight 里」来判断可以复位,
                #   而残留的登记恰好会让它跳过真正卡住的那些行
                for k, v in [(k, v) for k, v in list(self.inflight.items()) if time.time() - (v.get('since') or 0) > 7200]:
                    self.inflight.pop(k, None); db.log('warn', f'在途登记超过 2 小时未结束,已清理: {k}')
                stale = [r['id'] for r in db.q("select id from items where status='probing' and updated_at < ?", (time.time() - 7200,)) if r['id'] not in self.inflight]
                n = 0
                for c in range(0, len(stale), 900): n += db.x(f"update items set status='pending' where status='probing' and id in ({','.join('?'*len(stale[c:c+900]))})", stale[c:c+900])
                if n: db.log('warn', f'复位卡住的探测中条目 {n} 条')
                # 卡在 feeding 的(落库中途异常/进程被杀):没有入库在跑才动,回到 extracted 等重发
                if self.feed_lock.acquire(blocking=False):
                    try:
                        m = db.x("update items set status='extracted' where status='feeding' and updated_at < ?", (time.time() - 10800,))
                        m += db.x("update items set thumb='captured' where thumb='feeding' and updated_at < ?", (time.time() - 10800,))
                        if m: db.log('warn', f'复位卡住的入库中条目 {m} 条')
                    finally: self.feed_lock.release()
                # 「喂回后 Emby 仍无流记录」这一类要能自愈:成因几乎都是 Emby 当时在跑扫描/刷演员这类计划任务,
                # 刷新排队没来得及生效。事后去核实一遍,有流的直接标完成、没有的放回重发 ——
                # 实测一次「扫描媒体库 + 刷新中文演员」同时跑就打出 500 条这种失败,而事后抽查 60/60 其实都在 Emby 里
                if not self.feed_lock.locked() and time.time() - getattr(self, '_last_reverify', 0) > 3600:
                    if db.q("select 1 from items where status='failed' and error like '喂回:%' limit 1"):
                        self._last_reverify = time.time()
                        threading.Thread(target=self.reverify_failed, daemon=True, name='auto-reverify').start()
                if time.time() - float(db.setting('_last_sweep') or self.last_retry_sweep) > 86400:
                    # 「读不全」多半是当时被限流,不是文件真坏 → 每天自动放回去重试一次(只挑这一类错,不碰死链/坏文件)
                    self.last_retry_sweep = time.time(); db.set_setting('_last_sweep', self.last_retry_sweep)
                    # 只挑「读不全」这一类(多半是当时限流),每天最多 500 条,并且同一条最多放回 3 次——
                    # 真读不出来的文件每天重下一遍毫无意义,还会自己把限流冷却刷出来
                    ids = [r['id'] for r in db.q("select id from items where status='failed' and error like '%探测结果不完整%' and sweeps < 3 order by updated_at limit 500")]   # 最久没动的先来,否则永远在啃 id 最小的那批
                    if ids:
                        ph = ','.join('?' * len(ids))
                        # ⚠️顺序要紧:先放回截图、再放回媒体信息。
                        #   反过来写的话,两条 UPDATE 中间被杀会留下「status=pending + thumb=failed」——
                        #   扫尾的选择器要 status='failed'(已经不是了)、截图的选择器要 thumb='pending'(也不是),
                        #   两边都捡不起来,那批(最多 500 条)的封面就永久没了,而面板上的原因还写着「媒体信息提取失败,不截图」。
                        #   现在这个顺序下,中间被杀留下的是「thumb=pending + status=failed」,截图选择器照样能捡(它认 failed)
                        n2 = db.x(f"update items set thumb='pending', thumb_attempts=0, thumb_err=null where thumb='failed' and ext!='.iso' and id in ({ph})", ids)
                        db.x(f"update items set status='pending', attempts=0, sweeps=sweeps+1 where id in ({ph})", ids)
                        db.log('info', f'每天一次的重试:媒体信息 {len(ids)} 条、截图 {n2} 条已放回(同一条最多 3 次)')
                if time.time() - last_prune > 3600:
                    for f in glob.glob(os.path.join(OUT_DIR, '*.cand')):        # 截图中途被杀会留下 .cand
                        try:
                            if time.time() - os.path.getmtime(f) > 3600: os.remove(f)
                        except Exception: pass
                    self.sweep_out()
                    db.x('delete from metrics where ts < ?', (time.time() - 7 * 86400,)); db.x('delete from events where id < (select max(id) from events) - 5000'); last_prune = time.time()
                hrs = float(db.setting('worklist_sync_hours') or 0)
                # ⚠️计时基准取「上一次同步跑完的时刻」(同步任务自己落库的),不是本进程发起的时刻:
                #   用本地变量的话,撞上手动同步只能干等一个周期;而一发现没排上队就立刻重试,
                #   又会在人家刚跑完之后马上再全量快照一遍 library.db
                if hrs > 0 and time.time() - float(db.setting('_last_sync_at') or 0) > hrs * 3600 and not self.has_pending():
                    import main as _m; _m.worklist_sync()      # 待办清空后按周期自动同步新入库
                c = db.counts(); db.metric('done', c.get('done', 0)); db.metric('extracted_total', sum(v for k, v in c.items() if k in ('extracted', 'feeding', 'done', 'skipped')))
                if self.client: db.metric('api_rps', self.client.rps())
                db.metric('net_mbps', self.net['mbps'])
                if self.probe_times: db.metric('probe_p50_ms', statistics.median(self.probe_times[-100:]))
                if self.wal_due():
                    # ⚠️回收 WAL 之前先体检:库要是已经坏了,这一下等于把干净页往坏文件里写
                    q = db.quick_check()
                    if q != 'ok':
                        db.log('error', f'🔴 数据库自检不通过({str(q)[:120]}),已暂停并跳过 WAL 回收 —— 别再往里写,先备份 /config/state.sqlite')
                        db.set_setting('paused', 1)
                    else: db.x('pragma wal_checkpoint(TRUNCATE)')   # WAL 被常驻读者卡着不回收,一天能涨到 100M
            except Exception: pass
    def sweep_out(self):
        """暂存目录的兜底清扫:删掉「条目已经不需要它了」的 sidecar/jpg。
        ⭐为什么要兜底而不是逐处补:线上跑了几天后 out/ 里积了 725 个孤儿 jpg(约 187MB)——
          漏的那条路是 reconcile(它把 thumb 标成 done 却没删文件)。这种「某条路径忘了删」以后还会有。
        ⚠️只碰两小时没动过的文件:正在截的那张是先写文件、后改库,新文件绝不能扫掉"""
        import re as _re
        try: fs = os.listdir(OUT_DIR)
        except Exception: return
        cutoff = time.time() - 7200
        cand = {}
        for f in fs:
            m = _re.match(r'^(\d+)(\.json|-thumb\.jpg)$', f)
            if not m: continue
            p = os.path.join(OUT_DIR, f)
            try:
                if os.path.getmtime(p) > cutoff: continue
            except Exception: continue
            cand[p] = (int(m.group(1)), m.group(2))
        if not cand: return
        ids = sorted({v[0] for v in cand.values()})
        keep_json = set(); keep_jpg = set()
        for c in range(0, len(ids), 900):
            part = ids[c:c+900]; ph = ','.join('?' * len(part))
            for r in db.q(f"select id,status,thumb from items where id in ({ph})", part):
                # ⚠️failed 的 sidecar 也要留:入库失败重核靠它把条目放回 extracted,删了就只能重新探测一遍。
                #   json 很小(线上 1330 份共 4MB),占地的是 jpg
                if r['status'] in ('extracted', 'feeding', 'failed'): keep_json.add(r['id'])
                if r['thumb'] in ('captured', 'feeding'): keep_jpg.add(r['id'])
        n = 0
        for p, (i, kind) in cand.items():
            if i in (keep_json if kind == '.json' else keep_jpg): continue
            try: os.remove(p); n += 1
            except Exception: pass
        if n: db.log('info', f'暂存目录清扫:删掉 {n} 个已经用不上的文件')

    def wal_due(self):
        try:
            p = db.DB_PATH + '-wal'
            if os.path.getsize(p) > 64 * 1024 * 1024: return True
        except Exception: pass
        return False
    def status(self):
        c = db.counts(); total = sum(c.values())
        extracted = sum(c.get(k, 0) for k in ('extracted', 'feeding', 'done', 'skipped')) + c.get('failed', 0)   # 提取阶段已过的(含失败)
        ingested = c.get('done', 0) + c.get('skipped', 0)
        rate = self.rate_per_min()
        remaining = c.get('pending', 0) + c.get('probing', 0)
        eta = (remaining / rate * 60) if rate else None
        tc = {r['thumb']: r['n'] for r in db.q('select thumb, count(*) n from items group by thumb')}
        bc = {r['backdrop']: r['n'] for r in db.q('select backdrop, count(*) n from items where backdrop>0 group by backdrop')}
        ct = getattr(self, 'capture_times', [])
        return {'counts': c, 'total': total, 'extracted_total': extracted, 'ingested_total': ingested, 'thumb': tc, 'capture_mode': db.setting('capture_mode'),
                'backdrop': {'pending': bc.get(1, 0), 'done': bc.get(2, 0), 'failed': bc.get(3, 0)}, 'capture_p50_ms': int(statistics.median(ct[-100:])) if ct else None,
                'extract_pct': round(extracted * 100 / total, 2) if total else 0, 'ingest_pct': round(ingested * 100 / total, 2) if total else 0,
                'finished': ingested + c.get('failed', 0), 'mode': db.setting('mode'), 'phase': self.state['phase'], 'feed_phase': self.state.get('phase_feed', 'idle'),
                'auth': bool((db.try_settings(('auth_pass_hash',)) or {}).get('auth_pass_hash', True)),   # 读不到就当「已设口令」:宁可不提示,也别在首页喊「还没设口令」误导人 'cd2_saves': getattr(self, 'cd2_saves', 0), 'cooldown': max(0, int(getattr(self, 'cooldown_until', 0) - time.time())),
                'active_probes': self.slots.busy, 'queue': self.q.qsize() if hasattr(self, 'q') else 0, 'last_error': self.state['last_error'], 'last_feed': self.state['feed'],
                'rate_per_min': round(rate, 1), 'eta_seconds': int(eta) if eta else None,
                'probe_p50_ms': int(statistics.median(self.probe_times[-100:])) if self.probe_times else None,
                'api_rps': round(self.client.rps(), 2) if self.client else None,
                'breaker': {'tripped': bool(self.client and self.client.tripped_at), 'reason': self.client.reason if self.client else None, 'cookie_bad': bool(self.client and self.client.cookie_bad),
                            'since': self.client.tripped_at if self.client else None},
                'dir_cache': len(self.client.dir_cache) if self.client else 0, 'tree': (dict(self.client.tree_stats, loaded=list(self.client.tree.keys())) if self.client else {}), 'paused': bool(db.setting('paused')), 'cookie': dict(self.cookie),
                'net': {'mbps': self.net['mbps'], 'hist': self.net['hist'][-60:], 'total_rx': self.net['total_rx']},
                'inflight': sorted([{**v, 'elapsed': round(time.time() - v['since'], 1)} for v in list(self.inflight.values())], key=lambda v: v['since'])}
    def rate_per_min(self):
        t = time.time() - 600
        rows = db.q("select count(*) n from items where extracted_at > ?", (t,))
        return rows[0]['n'] / 10.0
PIPE = Pipeline()
