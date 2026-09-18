"""115 web API(cookie)封装:全局串行限速、目录缓存、批量直链、405/限流熔断"""
import time, threading, os, re, json, gzip, hashlib
import p115client
from config import CONFIG_DIR
from db import setting, log, metric

class Breaker(Exception): pass

def _scrub(msg):
    """115 的异常文本里常带完整直链(含签名),不能原样写进库再显示在 UI 和导出里"""
    return re.sub(r'https?://[^\s\'"]+', '<url>', str(msg))

class Client115:
    def __init__(self, cookie):
        self.c = p115client.P115Client(cookie)
        self.lock = threading.Lock(); self.last = 0.0
        self.brk_lock = threading.Lock()   # 只护熔断状态。⚠️不能借用 self.lock:那把锁要握住整个 115 请求(含限速 sleep),
                                           #   而 tripped() 在热循环里每轮都问,借过来就是把所有人串在一次网络请求后面
        self.tripped_at = None; self.reason = None; self.cookie_bad = None; self.consec_fail = 0
        self.dir_cache = {}; self.dir_lock = threading.Lock()
        self.fpath_cache = {}
        self.tree = {}; self.tree_lock = threading.Lock(); self.tree_load_lock = threading.Lock(); self.tree_stats = {'hits': 0, 'misses': 0, 'loads': 0}; self.prefetching = None
        self.calls = []   # 最近调用时间戳(算 rps)
    # ---- 熔断 ----
    def tripped(self):
        if self.tripped_at is None: return False
        if self.cookie_bad: return True                       # 登录失效不会自愈,等换 cookie
        if time.time() - self.tripped_at > 45*60:   # 冷却 45 分钟后允许再试一次
            # ⚠️要在锁里只放一个线程出去试探。不然冷却期一满,提取线程 + 截图线程 + 所有 worker 会同时冲出去
            #   (实测 8 个并发调用者全部拿到 False,还各打一条「尝试恢复」日志)—— 对着一个大概率还在生气的 WAF
            with self.brk_lock:
                if self.tripped_at is None: return False       # 别人已经放行了
                if time.time() - self.tripped_at <= 45*60: return True
                log('warn', '熔断冷却期满,放一个线程出去试探')
                self.tripped_at = None; self.reason = None; self.consec_fail = 0
            return False
        return True
    def _check(self, e, errno=None):
        """判是否被 115 限流/WAF 或登录失效:看 HTTP 状态码(兼容 urllib HTTPError 的 .code 和 httpx 的 .response)和中文错误语
        ⚠️ errno 必须按数字比:子串 "errno': 99" 会匹配到 990009 这类「稍后再试」的临时错,一旦误判成登录失效就永不自愈。
        (990001 本身确实是「登录超时,请重新登录」,按登录失效处理是对的)"""
        code = getattr(e, 'code', None) or getattr(e, 'status', None) or getattr(getattr(e, 'response', None), 'status_code', None)
        s = str(e)
        if errno is None:
            m = re.search(r"'errn?o'\s*:\s*(\d+)|\"errn?o\"\s*:\s*(\d+)", s, re.I)
            if m: errno = int(m.group(1) or m.group(2))
        login_bad = (errno in (99, 990001)) or any(k in s for k in ('请重新登录', '请重新登陆', '登录失效', '登陆失效', '登录已过期', '登录超时', '登陆超时', 'P115LoginError', 'not login', 'Not Login'))
        if login_bad:
            self.cookie_bad = s[:160]; log('error', '🔴 115 登录失效: ' + s[:160]); self.tripped_at = time.time(); self.reason = '登录失效,请更换 cookie'; return
        hit = code in (403, 405, 429) or errno == 770004 or any(k in s for k in ('770004', '访问上限', '访问被阻断', '操作太频繁', '请求过于频繁', '登录异常', 'Too Many Requests'))
        if hit:
            self.tripped_at = time.time(); self.reason = (f'HTTP {code} ' if code else '') + _scrub(s)[:160]; log('error', '🔴 熔断: ' + self.reason)
    def call(self, fn, *a, **k):
        if self.tripped(): raise Breaker(self.reason)
        with self.lock:
            iv = float(setting('api_interval'))
            w = iv - (time.time() - self.last)
            # ⚠️钳在一个间隔以内:时钟往回跳(NTP 校正,VPS 上很常见)会让这个差值变成几千秒,
            #   而这一觉是握着 115 全局锁睡的 —— 整条流水线跟着冻住(实测能算出 3601 秒)
            if w > 0: time.sleep(min(w, max(iv, 1.0)))
            try:
                r = fn(*a, **k)
                bad = isinstance(r, dict) and (r.get('state') is False or r.get('state') == 0)   # 0 is False 在 python 里是假,得分开判
                if not bad: self.consec_fail = 0
            except Exception as e:
                self._check(e)
                if not isinstance(e, FileNotFoundError):
                    self.consec_fail += 1
                    if self.consec_fail >= 15 and not self.tripped_at:     # 115 不可达/持续报错也算熔断,别让整个待办一条条被记失败
                        self.tripped_at = time.time(); self.reason = f'连续 {self.consec_fail} 次调用失败: {str(e)[:120]}'; log('error', '🔴 熔断: ' + self.reason)
                raise
            finally:
                self.last = time.time(); self.calls.append(self.last); self.calls = self.calls[-600:]
        if isinstance(r, dict) and (r.get('state') is False or r.get('state') == 0):
            try: errno = int(r.get('errno') or r.get('errNo') or r.get('code') or 0) or None
            except Exception: errno = None
            err = str(r.get('error') or r.get('error_msg') or r.get('message') or r.get('errno') or '')[:200]
            self._check(RuntimeError(err or str(r)[:200]), errno=errno)
            with self.lock:
                self.consec_fail += 1
                if self.consec_fail >= 15 and not self.tripped_at:
                    self.tripped_at = time.time(); self.reason = f'连续 {self.consec_fail} 次接口报错: {err[:120]}'; log('error', '🔴 熔断: ' + self.reason)
            raise RuntimeError('115 api: ' + (err or str(r)[:200]))
        return r
    def rps(self, window=60):
        t = time.time() - window
        return sum(1 for x in self.calls if x > t) / window
    # ---- 目录 ----
    def listing(self, d):
        ttl = float(setting('dir_cache_ttl'))
        with self.dir_lock:
            hit = self.dir_cache.get(d)
            if hit and time.time() - hit[0] < ttl: return hit[1]
        r = self.call(self.c.fs_dir_getid, d)
        try: cid = int(r.get('id') or 0)
        except Exception: cid = 0
        if not cid: raise FileNotFoundError('115 目录不存在: ' + d)   # id 可能是字符串 '0',必须转 int 再判
        items = {}; off = 0
        while True:
            r = self.call(self.c.fs_files, {'cid': cid, 'limit': 1000, 'offset': off, 'show_dir': 0, 'cur': 1})   # cur=1 只列本目录;cur=0 会递归子树,同名文件互相覆盖
            data = r.get('data', []) or []
            for it in data: items[it.get('n')] = it
            off += len(data)
            cnt = r.get('count')
            if not data or (cnt is not None and off >= int(cnt)) or (cnt is None and len(data) < 1000): break   # count 缺失时按整页判断,别只列第一页
        with self.dir_lock:
            self.dir_cache[d] = (time.time(), items)
            # ⚠️上限只数「目录个数」是不够的:每个 value 是整目录的 115 原始记录(每条 40 来个字段),
            #   实测每目录 12~40 个文件时,4000 个目录就是 53~173MB 常驻。先按 TTL 扔过期的,再按个数兜底
            if len(self.dir_cache) > 500:
                dead = time.time() - float(setting('dir_cache_ttl') or 3600)
                for k in [k for k, v in list(self.dir_cache.items()) if v[0] < dead]: self.dir_cache.pop(k, None)
            if len(self.dir_cache) > 4000:                          # 有界:待办按路径排序,老目录不会再用
                for k in sorted(self.dir_cache, key=lambda k: self.dir_cache[k][0])[:1000]: self.dir_cache.pop(k, None)
        return items
    # ---- 分类树缓存:文件条目可递归一次拿整个分类(每页 1150),把「每条 2 次 API」压成「每千条 1 次」 ----
    @staticmethod
    def category_of(rel):
        """/媒体/电影/华语/片名 (2013) {tmdb-1}/x.mkv → /媒体/电影/华语(前 3 段);太浅的路径不走树"""
        parts = rel.strip('/').split('/')
        return '/' + '/'.join(parts[:3]) if len(parts) > 4 else None
    def _touch(self, cat):
        """命中就刷新时间戳(调用方需持 tree_lock)。
        ⚠️不刷的话这就不是 LRU 而是「按装入顺序淘汰」:手动重试会把别的分类顶到队首 →
          预取跟着换分类 → 正在用的那棵树反而被挤掉,而重建一棵是几百次 115 调用(每次至少 1 秒)"""
        t = self.tree.get(cat)
        if t: t['at'] = time.time()
        return t

    def load_tree(self, cat):
        with self.tree_lock:
            t = self._touch(cat)
            if t: return t
        with self.tree_load_lock:                                 # 同一时间只加载一个分类(提取线程与截图线程可能同时要)
            with self.tree_lock:
                t = self._touch(cat)
                if t: return t
            return self._load_tree(cat)
    def _tree_file(self, cat):
        d = os.path.join(CONFIG_DIR, 'tree'); os.makedirs(d, exist_ok=True)
        return os.path.join(d, hashlib.md5(cat.encode()).hexdigest()[:16] + '.json.gz')
    def _load_tree(self, cat):
        fp = self._tree_file(cat)
        try:                                                    # 磁盘缓存 24h 内直接用(容器重启/重新部署不用再拉几十分钟)
            if os.path.exists(fp) and time.time() - os.path.getmtime(fp) < 7 * 86400:   # 一周。树过期有「解析对不上就作废重建」兜底,而重建一次是十几分钟全线停摆
                with gzip.open(fp, 'rt', encoding='utf-8') as f: t = json.load(f)
                if t.get('cat') == cat:
                    files = {k: [tuple(x) for x in v] for k, v in t['files'].items()}
                    tree = {'folders': t['folders'], 'files': files, 'n': t['n'], 'at': time.time()}
                    with self.tree_lock:
                        self.tree[cat] = tree; self.tree_stats['loads'] += 1
                        if len(self.tree) > 2:
                            oldest = min(self.tree, key=lambda k: self.tree[k]['at']); self.tree.pop(oldest, None)
                    log('info', f'分类树从磁盘缓存加载 {cat}: 文件 {t["n"]}'); return tree
        except Exception as e: log('warn', f'分类树磁盘缓存读取失败 {cat}: {str(e)[:100]}')
        r = self.call(self.c.fs_dir_getid, cat)
        try: cid = int(r.get('id') or 0)
        except Exception: cid = 0
        if not cid: raise FileNotFoundError('115 目录不存在: ' + cat)
        folders = {}; off = 0                                   # 直接子目录(电影目录 / 剧目录):id → 名
        while True:
            r = self.call(self.c.fs_files, {'cid': cid, 'limit': 1150, 'offset': off, 'show_dir': 1, 'cur': 1})
            data = r.get('data', []) or []
            for x in data:
                if not x.get('fid'): folders[str(x.get('cid'))] = x.get('n')
            off += len(data); cnt = r.get('count')
            if not data or (cnt and off >= int(cnt)) or (not cnt and len(data) < 1150): break
        files = {}; off = 0; n = 0                              # 整个子树的文件:文件名 → [(fid, pc, size, sha, parent_cid)]
        while True:
            r = self.call(self.c.fs_files, {'cid': cid, 'limit': 1150, 'offset': off, 'show_dir': 0, 'cur': 0})
            data = r.get('data', []) or []
            for x in data:
                files.setdefault(x.get('n'), []).append((str(x.get('fid')), x.get('pc'), int(x.get('s') or 0), x.get('sha') or '', str(x.get('cid'))))
            off += len(data); n += len(data); cnt = r.get('count')
            if not data or (cnt and off >= int(cnt)) or (not cnt and len(data) < 1150): break   # count 缺失/为 0 时按整页判断,否则半截树会配错文件
        tree = {'folders': folders, 'files': files, 'n': n, 'at': time.time()}
        with self.tree_lock:
            self.tree[cat] = tree; self.tree_stats['loads'] += 1
            if len(self.tree) > 2:                                # 最多留 2 个分类(待办按路径排序,用完即弃)
                oldest = min(self.tree, key=lambda k: self.tree[k]['at']); self.tree.pop(oldest, None)
        try:
            with gzip.open(fp, 'wt', encoding='utf-8') as f: json.dump({'cat': cat, 'folders': folders, 'files': files, 'n': n}, f, ensure_ascii=False)
            # 顺手清掉超过 3 天没动的:这些 gz 只增不减,一个大分类几十 MB(线上 13 个已占 59MB)
            d = os.path.dirname(fp); cut = time.time() - 3 * 86400
            for x in os.listdir(d):
                q = os.path.join(d, x)
                try:
                    if x.endswith('.json.gz') and q != fp and os.path.getmtime(q) < cut: os.remove(q)
                except Exception: pass
        except Exception as e: log('warn', f'分类树写磁盘缓存失败: {str(e)[:100]}')
        log('info', f'分类树已加载 {cat}: 目录 {len(folders)} 文件 {n}')
        return tree
    def drop_tree(self, cat):
        """作废某个分类的树(内存+磁盘):发现解析对不上时用,下次会重新拉"""
        if not cat: return
        with self.tree_lock: self.tree.pop(cat, None)
        try: os.remove(self._tree_file(cat))
        except Exception: pass
        log('warn', f'分类树已作废,下次重建: {cat}')
    def prefetch(self, cat):
        """后台预取下一个分类的树,让加载与探测重叠"""
        with self.tree_lock:                                      # 判断要在锁里做,否则和淘汰逻辑赛跑
            if not cat or cat in self.tree or self.prefetching: return
            self.prefetching = cat
        def job():
            try: self.load_tree(cat)
            except Exception as e: log('warn', f'预取分类树失败 {cat}: {str(e)[:120]}')
            finally: self.prefetching = None
        threading.Thread(target=job, daemon=True, name='tree-prefetch').start()
    def folder_path(self, folder_id):
        """按目录 id 取完整路径(1 次调用,结果缓存:一个季目录问一次,整季的集都靠它核对)"""
        fid = str(folder_id)
        with self.dir_lock:
            hit = self.fpath_cache.get(fid)
        if hit: return hit
        r = self.call(self.c.fs_files, {'cid': int(folder_id), 'limit': 1, 'offset': 0, 'show_dir': 1, 'cur': 1})
        crumbs = r.get('path') or []
        p = '/' + '/'.join(str(x.get('name')) for x in crumbs if str(x.get('cid')) not in ('0',))
        if p in ('', '/'): return p                                  # 面包屑没拿到,别把退化结果缓存一辈子
        with self.dir_lock:
            self.fpath_cache[fid] = p
            if len(self.fpath_cache) > 20000:
                for k in list(self.fpath_cache)[:5000]: self.fpath_cache.pop(k, None)
        return p
    def resolve_tree(self, rel):
        cat = self.category_of(rel)
        if not cat: return None
        # ⚠️必须走 _touch:稳态下解析每条都走这里,而不是 load_tree。
        #   直接 .get() 的话「正在用的那棵树」时间戳永远停在装入时刻,
        #   第三棵树一装进来(手动重试/drop_tree 重建/扫尾截图),被淘汰的正好是它 —— 重建一次十几分钟
        with self.tree_lock: tree = self._touch(cat)
        tree = tree or self.load_tree(cat)
        d, fn = os.path.split(rel); cands = tree['files'].get(fn) or []
        if not cands: self.tree_stats['misses'] += 1; return None
        sub = d[len(cat):].strip('/').split('/')                # 分类下的相对目录段
        if len(sub) == 1:                                       # 电影:分类/片目录/文件 → 父目录名就在树里,直接精确匹配
            cands = [c for c in cands if tree['folders'].get(c[4]) == sub[0]]
        else:                                                   # 剧集:父目录是「Season xx」,树里没有它的路径 → 查一次(按目录缓存,整季只问一次)
            # 🔴 单个候选也必须核对:树可能是旧的/截断的,真正的文件不在树里时,同名的另一部剧会被当成它
            verified = []
            for c in cands:
                try:
                    if self.folder_path(c[4]) == d: verified.append(c)
                except Breaker: raise
                except Exception as e: log('warn', f'核对父目录失败,退回目录列举: {str(e)[:100]}'); return None
            cands = verified
        if len(cands) != 1: self.tree_stats['misses'] += 1; return None
        self.tree_stats['hits'] += 1
        fid, pc, size, sha, _ = cands[0]
        return fid, pc, size, sha
    def resolve(self, rel):
        """/媒体/.../file.mkv -> (fid, pickcode, size, sha1):先查分类树,没命中再按目录列举(也能兜住树加载后新入库的文件)"""
        try:
            r = self.resolve_tree(rel)
            if r: return r
        except Breaker: raise
        except FileNotFoundError: raise
        except Exception as e: log('warn', f'分类树查找异常,退回目录列举: {str(e)[:120]}')
        d, fn = os.path.split(rel); it = self.listing(d).get(fn)
        if not it: raise FileNotFoundError('115 目录里没有该文件: ' + rel)
        return str(it.get('fid')), it.get('pc'), int(it.get('s') or 0), (it.get('sha') or '')
    # ---- 直链 ----
    def urls(self, pickcodes, ua):
        """批量取直链 → {pickcode|fid: url}。批量接口是全有全无的(一个坏文件整批抛错),失败时逐个回退,只让坏的那个失败"""
        pickcodes = list(pickcodes)
        if not pickcodes: return {}
        out = {}; meta = {}
        def take(k, u):
            m = dict(getattr(u, 'mapping', {}) or {})
            pc = m.get('pickcode') or getattr(u, 'pickcode', None)
            if pc: out[pc] = str(u); meta[pc] = m
            out[str(k)] = str(u); meta[str(k)] = m
        try:
            r = self.call(self.c.download_urls, pickcodes, user_agent=ua)
            for k, u in r.items(): take(k, u)
            out['__meta__'] = meta
            return out
        except Breaker: raise
        except Exception as e:
            if len(pickcodes) == 1: return {'__errors__': {pickcodes[0]: _scrub(e)[:120]}}
            log('warn', f'批量直链失败({_scrub(e)[:80]}),改为逐个取')
        for pc in pickcodes:
            try: take(pc, self.call(self.c.download_url, pc, user_agent=ua))
            except Breaker:
                out['__meta__'] = meta; raise
            except Exception as e: out.setdefault('__errors__', {})[pc] = _scrub(e)[:120]
        out['__meta__'] = meta   # 🔴回退路径也要带上元数据,否则名字校验会静默失效
        return out
