"""在「Emby 主机」上执行的两段逻辑,同机模式直接 import,远程模式作为脚本推过去跑。
纯标准库,不依赖本项目其它文件。

cfg 字段:
  emby_url, emby_api_key, emby_user_id
  library_db        Emby 的 library.db 路径(本机可读)
  emby_strm_prefix  Emby 库里 strm 条目 Path 的前缀(Emby 容器内路径,如 /strm)
  strm_root         该前缀在「本机」对应的真实目录(如 /mnt/media/strm)
  mediainfo_root    神医「媒体信息」根目录(本机真实路径,如 /mnt/media/mediainfo)
"""
import os, re, json, sqlite3, time, tarfile, urllib.request, tempfile, base64

# ---------------- 待办清单 ----------------
def build_worklist(cfg, snapshot_dir=None):
    """库快照(VACUUM INTO,只读连接不锁活库) → 无媒体信息的 strm 类 Movie/Episode。返回 (rows, meta)"""
    t0 = time.time()
    src = cfg['library_db']
    snap = os.path.join(snapshot_dir or tempfile.gettempdir(), '.emby-mediainfo-snapshot.db')
    if os.path.exists(snap): os.remove(snap)
    c = sqlite3.connect('file:' + urllib.request.pathname2url(src) + '?mode=ro', uri=True)
    try:
        c.execute("VACUUM INTO '" + snap.replace("'", "''") + "'")          # SQLite ≥3.27
    except sqlite3.OperationalError:
        dst = sqlite3.connect(snap); c.backup(dst); dst.close()             # 老版本走 backup API
    c.close()
    s = sqlite3.connect(f'file:{snap}?mode=ro', uri=True)
    try:
        return _scan_worklist(s, cfg, total_of=snap, t0=t0)
    finally:
        # ⚠️关连接和删快照必须在 finally:行循环里抛一次异常就漏一个连接,
        #   而那份快照是整个 library.db 的副本(线上 5GB),会一直躺到下次同步开头
        try: s.close()
        except Exception: pass
        try: os.remove(snap)
        except Exception: pass

def _scan_worklist(s, cfg, total_of=None, t0=None):
    t0 = t0 or time.time()
    s.execute("create temp table hasms as select distinct ItemId from MediaStreams2")
    total = s.execute("select count(*) from MediaItems where type in (5,8) and Path like '%.strm'").fetchone()[0]
    pre, root = cfg['emby_strm_prefix'].rstrip('/'), cfg['strm_root'].rstrip('/')
    rows = []; unreadable = 0; unreadable_ids = []; outside = 0
    want_thumb = bool(cfg.get('want_thumb'))
    # 缺封面的剧集和电影都截(与神医/Emby 原生一致:哪个缺封面截哪个);电影缺海报时顺带看有没有背景图,没有就把截图也当背景图(神医 GenerateBackdrop,剧集不做)
    # Images 列的真实格式(线上核过):`路径*时间戳*类型*宽*高*null`,多张用 | 分隔。
    # ⚠️匹配要带星号:直接 like '%Primary%' 会被**路径里的字**命中(《Primary Colors》这种片名),
    #   结果是「明明没海报却被当成有」,静默漏掉不截图
    sql = ("select Id,type,Name,Path, Id in (select ItemId from hasms) as has_mi, (Images like '%*Primary*%') as has_primary, (Images like '%*Backdrop*%') as has_backdrop from MediaItems "
           "where type in (5,8) and Path like '%.strm' and (Id not in (select ItemId from hasms)" + (" or (Images is null or Images not like '%*Primary*%')" if want_thumb else '') + ")")
    for Id, t, name, path, has_mi, has_primary, has_backdrop in s.execute(sql):
        if not path.startswith(pre + '/'):
            # ⚠️这不是「读不到」,是「不在我们负责的前缀下」—— 很可能是另一个 strm 媒体库。
            #   当年混在 unreadable 里的后果是结构性的:只要那个库存在,每次同步都触发「清单异常」守卫,
            #   「把已完成的标 skipped」这一步就永远不执行,待办只增不减
            outside += 1
            continue
        local = root + path[len(pre):]
        try:
            # utf-8-sig:带 BOM 的 strm 会让前缀判断整条失败,而报错是「内容不以 115 前缀开头」,指错方向
            with open(local, encoding='utf-8-sig', errors='replace') as fh: tgt = fh.readline().strip()
        except Exception:
            unreadable += 1
            if len(unreadable_ids) < 20000: unreadable_ids.append(Id)
            continue
        if not tgt or '\ufffd' in tgt:
            # 空文件、或非 UTF-8(GBK 路径)解出来的替换字符 —— 发下去必然解析失败并烧光重试次数,
            # 而错误会显示成「115 目录里没有该文件」,永远查不到真因。当读不到处理
            unreadable += 1
            if len(unreadable_ids) < 20000: unreadable_ids.append(Id)
            continue
        rows.append({'id': Id, 'kind': 'Movie' if t == 5 else 'Episode', 'name': name, 'strm': path, 'target': tgt,
                     'has_mediainfo': bool(has_mi), 'need_thumb': not has_primary, 'need_backdrop': (t == 5 and not has_primary and not has_backdrop)})
    return rows, {'total_strm_items': total, 'no_mediainfo': len(rows), 'unreadable': unreadable, 'unreadable_ids': unreadable_ids,
                  'outside_prefix': outside,      # 不归本前缀管的(另一个媒体库),单独报,不进异常判定
                  'unreadable_truncated': len(unreadable_ids) < unreadable, 'seconds': round(time.time() - t0)}

# ---------------- Emby API ----------------
def _clean(msg, cfg=None):
    """错误文本脱敏:去掉 api_key 和完整 URL,免得写进库又显示在 UI 上"""
    m = str(msg)
    if cfg and cfg.get('emby_api_key'): m = m.replace(cfg['emby_api_key'], '***')
    return re.sub(r'https?://[^\s\'"]+', '<url>', m)
def _get(cfg, path, timeout=30):
    url = cfg['emby_url'].rstrip('/') + path + ('&' if '?' in path else '?') + 'api_key=' + cfg['emby_api_key']
    with urllib.request.urlopen(url, timeout=timeout) as r: return json.loads(r.read().decode('utf-8'))
def emby_info(cfg):
    info = _get(cfg, '/emby/System/Info', 15)
    sid = info.get('Id')
    users = [{'id': u['Id'], 'name': u['Name'], 'admin': bool((u.get('Policy') or {}).get('IsAdministrator'))} for u in _get(cfg, '/emby/Users', 15)]
    return {'server': info.get('ServerName'), 'version': info.get('Version'), 'server_id': sid, 'users': users}
def latency(cfg):
    t = time.time()
    try: _get(cfg, '/emby/Items/Counts'); return round(time.time() - t, 3)
    except Exception: return 99.0
def item_state(cfg, item_id):
    """(流数, 有无 Primary 图, 有无背景图);查询出错 → (-1, False, False)"""
    try:
        d = _get(cfg, f"/emby/Users/{cfg['emby_user_id']}/Items/{item_id}?Fields=MediaStreams")
        return len(d.get('MediaStreams') or []), bool((d.get('ImageTags') or {}).get('Primary')), bool(d.get('BackdropImageTags'))
    except Exception: return -1, False, False
def upload_image(cfg, item_id, itype, data):
    """把图片交给 Emby 存(POST /Items/{id}/Images/{Type},正文 base64)。Emby 按媒体库设置决定落在媒体旁边还是 metadata 目录,与原生截图同一条路"""
    url = cfg['emby_url'].rstrip('/') + f"/emby/Items/{item_id}/Images/{itype}?api_key={cfg['emby_api_key']}"
    req = urllib.request.Request(url, method='POST', data=base64.b64encode(data), headers={'Content-Type': 'image/jpeg'})
    return urllib.request.urlopen(req, timeout=120).getcode()
def split_images(files):
    """files 里 'img/<id>.jpg' 是要经 API 上传的图(电影海报),不落盘;返回 {id: bytes}"""
    out = {}
    for rel, content in files.items():
        if rel.startswith('img/'):
            try: out[int(os.path.basename(rel).split('.')[0])] = content if isinstance(content, bytes) else content.encode('utf-8')
            except ValueError: pass
    return out
def refresh(cfg, item_id):
    url = cfg['emby_url'].rstrip('/') + f"/emby/Items/{item_id}/Refresh?Recursive=false&MetadataRefreshMode=Default&ImageRefreshMode=Default&ReplaceAllMetadata=false&ReplaceAllImages=false&api_key={cfg['emby_api_key']}"
    return urllib.request.urlopen(urllib.request.Request(url, method='POST', data=b''), timeout=60).getcode()

# ---------------- 入库 ----------------
def write_sidecars(cfg, files, errors=None, progress=None):
    """files: {相对路径: 内容}。路径以 'mediainfo/' 开头 → 媒体信息根;以 'strm/' 开头 → strm 根(截图 -thumb.jpg 放媒体旁边);内容 str 或 bytes
    单个文件写失败(名字超长/权限/满盘)只记进 errors,不能让整批翻车——否则回退重发会无限循环同一批"""
    n = 0; errors = errors if errors is not None else {}
    for k, (rel, content) in enumerate(files.items()):
        # 🔴这里要打心跳:一批 2000 个文件全写完才有下一行输出,写的还是网络/FUSE 共享,
        #   本地看着像「远端 N 秒没动静」→ 判死 → 回滚整批 → 下一批把同样的条目再在生产 Emby 上刷一遍
        if progress and k and k % 25 == 0: progress(f'write {k}/{len(files)}')   # 写的是网络/FUSE 共享,一个文件卡几秒很正常,间隔不能大
        if rel.startswith('img/'): continue                                   # 经 API 上传的图,不落盘
        if rel.startswith('mediainfo/'): root, sub = cfg['mediainfo_root'], rel[len('mediainfo/'):]
        elif rel.startswith('strm/'): root, sub = cfg['strm_root'], rel[len('strm/'):]
        else: root, sub = cfg['mediainfo_root'], rel
        if sub.startswith('/'): errors[rel] = '绝对路径,拒绝写入'; continue      # 正常成员名都是相对的
        dst = os.path.join(root, sub)
        # 🔴包含性检查:成员名里带 ../ 时要拦住(这里是以 root 身份写文件)。
        #    ⚠️用 normpath 而不是 realpath:媒体目录里有符号链接是很常见的(把某个分类挪到别的盘时通常就是一个软链),
        #    realpath 会把这些正常情况也判成越界,导致整批写不进去。
        rp, rr = os.path.normpath(dst), os.path.normpath(root)
        if rp != rr and not rp.startswith(rr.rstrip('/') + os.sep):
            errors[rel] = '路径越界,拒绝写入'; continue
        try:
            # 🔴根目录不存在就直接拒,别用 makedirs 把它建出来:
            #   挂载没挂上时原来会安静地在容器自己的盘上建出整棵树、写进去、然后「一份都写不进去」那道守卫不触发,
            #   Emby 当然读不到 → 整批 unverified → 被判成「Emby 正忙」不计次数 → 无限重刷生产 Emby。
            #   更毒的是路径体检会因为目录被建出来而从红翻绿
            if not os.path.isdir(root):
                errors[rel] = f'根目录不存在(挂载没挂上?): {root}'; continue
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            # O_NOFOLLOW:normpath 看不见符号链接,最后一段要是个软链就会写到别处去(这里是 root 身份)
            fd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o644)
            with os.fdopen(fd, 'wb') as f:
                f.write(content if isinstance(content, bytes) else content.encode('utf-8'))
                os.fchmod(f.fileno(), 0o644)   # 用 fchmod 而不是 chmod:后者会跟随软链,等于把刚堵上的洞又开一半
            n += 1
        except OSError as e:
            if getattr(e, 'errno', None) in (40, 62, 92):      # ELOOP/太多层软链:是我们主动拒绝,不是磁盘坏了
                errors[rel] = '目标是符号链接,按策略拒绝写入'
            else: errors[rel] = _clean(e, cfg)[:160]
        except Exception as e: errors[rel] = _clean(e, cfg)[:160]
    return n
def ingest(cfg, ids, interval=0.3, latency_threshold=1.0, thumb_ids=None, progress=None, images=None, backdrop_ids=None, files=None, thumb_files=None):
    """逐条 Refresh(限速+延迟看门狗),先 GET 判「Emby 已有→跳过」,末尾 API 复核。
    thumb_ids:这批里带截图、要复核 Primary 图的 id。查询出错(-1)一律记 unverified,不当失败。progress:每 20 条回调(远程模式打心跳)
    images:{id: jpg bytes} 经 API 上传当海报的(电影);backdrop_ids:其中没有背景图时同一张也上传为背景图的 id
    files:要落盘的文件(媒体信息 json 等),预检前就写;thumb_files:{id: 相对路径},只给预检时确认仍缺封面的条目落盘(别覆盖人家已有的剧照)"""
    t0 = time.time(); res = {'done': [], 'skipped': [], 'failed': {}, 'unverified': [], 'thumb_done': [], 'thumb_failed': [], 'thumb_unverified': [],
                             'backdrop_done': [], 'backdrop_failed': {}, 'lat': [latency(cfg)], 'slowdowns': 0}
    thumb_ids = set(thumb_ids or []); images = images or {}; backdrop_ids = set(backdrop_ids or []); todo = []; only_thumb = []; pre_state = {}
    files = dict(files or {}); thumb_files = {int(k): v for k, v in (thumb_files or {}).items()}
    res['write_failed'] = {}
    thumb_rels = set(thumb_files.values())
    if files: write_sidecars(cfg, {k: v for k, v in files.items() if k not in thumb_rels}, res['write_failed'], progress)
    # 🔴一份都写不进去(媒体信息目录只读/满盘)就别往下走了。
    #   原来会照样对着 2000 条发 Refresh —— 实测 2000 次 Refresh + 约 4 万次 GET 全是白费,
    #   最后整批判 failed,而那个错误既不进每日重试也不进重核,只能人工点按钮。
    #   改成整批回退(unverified):挂载修好后自动重发,而且不扣重试次数
    if files and len(res['write_failed']) >= max(3, 0.9 * len([k for k in files if not k.startswith('img/')])):
        res['unverified'] = list(ids)
        res['seconds'] = round(time.time() - t0, 1)
        res['fatal'] = '媒体信息目录写不进去(只读/满盘?),本批整批退回,没有对 Emby 做任何刷新'
        return res
    uploaded = set()
    for k, i in enumerate(ids):
        n, has_p, has_bd = item_state(cfg, i); pre_state[i] = (n, has_p, has_bd)
        if n > 0:
            res['skipped'].append(i)
            if i in thumb_ids and not has_p: only_thumb.append(i)   # 媒体信息已有,只为截图刷一次
        else: todo.append(i)                                       # 0 或 -1(未知)都刷一遍,刷新是幂等的
        if progress and (k + 1) % 20 == 0: progress(f'precheck {k + 1}/{len(ids)}')   # 每条最坏 30s(GET 超时),间隔太大会撞上本地的空闲超时
    todo = todo + only_thumb
    # 剧集截图:预检说还缺封面的才落盘(Emby 这会儿已经有图了就别放我们的截图进去覆盖)
    skipped_thumbs = {}
    for k, (i, rel) in enumerate(thumb_files.items()):
        n, has_p, _ = pre_state.get(i, (-1, False, False))
        if has_p: skipped_thumbs[i] = 'Emby 已有封面,不覆盖'; thumb_ids.discard(i)
        elif rel in files:
            write_sidecars(cfg, {rel: files[rel]}, res['write_failed'])
            if rel in res['write_failed']: thumb_ids.discard(i)     # 图没落盘,别再给它「刷新后没图」的第二个结论
        # 🔴心跳必须打在这个循环里:这里是一条一条调 write_sidecars(每次只有一个文件),
        #   把回调传进去是没用的 —— 它的计数器是「这一次调用内写了几个」,永远到不了阈值。
        #   而这一段可能连写几百张图到网络/FUSE 共享上,中间一声不吭 = 本地判远端死了 = 整批白跑还要重刷一遍生产库
        if progress and (k + 1) % 20 == 0: progress(f'thumb-write {k + 1}/{len(thumb_files)}')
    res['thumb_skipped'] = skipped_thumbs
    # 电影海报走 API 上传(Emby 自己决定存哪、顺带出 tag);已经有海报/背景图的绝不覆盖,与原生「只给缺图的截」一致
    upload_errs = 0
    for k, i in enumerate(sorted(images)):
        n, has_p, has_bd = pre_state.get(i, (-1, False, False))
        if n < 0: continue                                         # 状态未知别上传,复核阶段会记 unverified 下次再来
        try:
            if has_p: skipped_thumbs[i] = 'Emby 已有封面,不覆盖'; thumb_ids.discard(i)   # 和剧集同一口径:不覆盖别人的图,也别谎报成「已上传」
            else:
                code = upload_image(cfg, i, 'Primary', images[i])
                if code not in (200, 204): raise RuntimeError(f'http {code}')
                uploaded.add(i); upload_errs = 0                   # 🔴只有真传了才算一次成功,否则「连续 5 次失败」这道闸会被夹在中间的「本来就有图」重置掉
            if i in backdrop_ids and not has_bd:
                try:
                    code = upload_image(cfg, i, 'Backdrop', images[i])
                    if code not in (200, 204): raise RuntimeError(f'http {code}')
                    res['backdrop_done'].append(i); upload_errs = 0   # 上传是同步的,成功即算数(别等刷新后再查,查早了会误判)
                except Exception as e:
                    res['backdrop_failed'][i] = _clean(e, cfg)[:120]; upload_errs += 1   # 背景图失败也要计入连败,否则这条路没有闸
                    if upload_errs >= 5:
                        res.setdefault('thumb_errors', {})['*'] = '连续 5 次上传失败,本批停止传图'
                        for j in sorted(images):     # 海报已经传成功的不能被算成失败,只把没轮到的顺延
                            if j not in uploaded and j in thumb_ids and j != i: res['thumb_unverified'].append(j); thumb_ids.discard(j)
                        break
            elif i in backdrop_ids and has_bd: res['backdrop_done'].append(i)
        except Exception as e:                                     # 传图失败不影响媒体信息入库;记 unverified 下批重来,连着失败就整批停手(多半是 Emby 挂了或 key 不对)
            res['thumb_unverified'].append(i); res.setdefault('thumb_errors', {})[i] = '海报上传: ' + _clean(e, cfg)[:120]
            thumb_ids.discard(i); res['backdrop_failed'].pop(i, None); upload_errs += 1
            if upload_errs >= 5:
                for j in sorted(images):
                    if j != i and j not in uploaded and j in thumb_ids: res['thumb_unverified'].append(j); thumb_ids.discard(j)
                res.setdefault('thumb_errors', {})['*'] = '连续 5 次上传失败,本批停止传图'
                break
        time.sleep(interval)
        if progress and (k + 1) % 5 == 0: progress(f'upload {k + 1}/{len(images)}')   # 一条最坏 240s(海报+背景图各 120s 超时),20 条一跳能跳出本地的空闲超时
    for k, i in enumerate(todo):
        try:
            code = refresh(cfg, i)
            if code not in (200, 204): res['failed'][i] = f'refresh http {code}'
        except Exception as e: res['failed'][i] = 'refresh ' + _clean(e, cfg)[:120]
        time.sleep(interval)
        if (k + 1) % 20 == 0:
            l = latency(cfg); res['lat'].append(l)
            if progress: progress(f'refresh {k + 1}/{len(todo)} lat={l}')
            while l > latency_threshold:
                res['slowdowns'] += 1
                if res['slowdowns'] > 60: raise RuntimeError(f'Emby 持续不可用或延迟过高({l}s),中止本批,已刷新 {k + 1}/{len(todo)}')   # 最多等 10 分钟
                if progress: progress(f'waiting emby lat={l} ({res["slowdowns"]}/60)')
                time.sleep(10); l = latency(cfg); res['lat'].append(l)
    # 复核:Emby 的 Refresh 是异步排队的,一批几千条时后面的要等一会才处理;查不到就隔 10 秒再查,最多等 3 分钟
    post = {}; pending_ids = [i for i in todo if i not in res['failed']]
    for attempt in range(19):
        time.sleep(3 if attempt == 0 else 10)
        still = []
        for c, i in enumerate(pending_ids):
            n, has_p, has_bd = item_state(cfg, i)
            pn, pp, pb = post.get(i, (-1, False, False))
            post[i] = (n if n >= 0 else pn, has_p or pp, has_bd or pb)   # 🔴查到过的好结果不许被后来一次超时擦掉(否则明明已入库却记成 unverified,攒三次判失败)
            n, has_p, has_bd = post[i]
            if progress and (c + 1) % 50 == 0: progress(f'verify {attempt + 1}: {c + 1}/{len(pending_ids)}')
            need_mi = (i not in only_thumb) and n <= 0                       # 媒体信息还没进库;n=-1 是这次查询超时,也要继续轮(判成 unverified 的话整条下批重来一遍)
            # 图是异步扫的,来得比流记录晚 → 值得等,但只等前 6 轮(约 1 分钟);查询出错或我们自己传过的就别轮了
            need_th = (i in thumb_ids) and (i not in uploaded) and n >= 0 and not has_p and attempt < 6
            if need_mi or need_th: still.append(i)
        if progress: progress(f'verify attempt {attempt + 1}: {len(pending_ids) - len(still)}/{len(pending_ids)} ok')
        pending_ids = still
        if not pending_ids: break
    for i in todo:
        if i in res['failed'] or i in only_thumb: continue
        n, has_p, _ = post.get(i) or pre_state.get(i) or (-1, False, False)
        if n > 0: res['done'].append(i)
        elif n == 0: res['unverified'].append(i)                   # 等了 3 分钟仍无流:可能还在排队/被判异常,回退重发(幂等),别直接判失败
        else: res['unverified'].append(i)
    unsure = set(res['unverified']) | set(int(k) for k in res['failed'])   # 媒体信息本身没落实的,截图结论也不算数(下批连图一起重发,而且不该记在截图账上)
    res['thumb_deferred'] = []
    for i in thumb_ids:                                            # 截图:每个都给结论,不留 feeding
        n, has_p, has_bd = post.get(i) or pre_state.get(i) or (-1, False, False)
        if has_p or i in uploaded: res['thumb_done'].append(i)
        elif i in unsure: res['thumb_deferred'].append(i)           # 不是图的问题 → 顺延,不扣重试次数
        else: res['thumb_unverified'].append(i)                     # 图还没被 Emby 认下:重发同一张(文件还在本地),次数满了才退场
        if i in backdrop_ids and i not in res['backdrop_failed'] and i not in res['backdrop_done'] and has_bd: res['backdrop_done'].append(i)
    res['seconds'] = round(time.time() - t0, 1)
    return res
def _heartbeat(stop, every=60):
    """🔴定时心跳,和进度无关。
    进度心跳只能在「两件事之间」打 —— 单次写入卡在网络/FUSE 共享上几十分钟时,它一声都打不出来,
    本地就会判「远端死了」把整批回滚,而远端其实还在跑(线上真出过一次:2400 秒无输出,
    回滚后下一批发现那 1994 条早就写好了)。定时线程能把「慢」和「死」区分开。"""
    import threading, sys, time as _t
    def run():
        while not stop.is_set():
            if stop.wait(every): return
            try: print('# alive', flush=True)
            except Exception: return
    th = threading.Thread(target=run, daemon=True); th.start()
    return th

def ingest_tar(cfg, tgz):
    """远程模式:解包 tgz(内含 _manifest.json)→ ingest(落盘时机由 ingest 决定:截图要先确认条目仍缺封面)"""
    try:
        with tarfile.open(tgz, 'r:gz') as t:
            man = json.load(t.extractfile('_manifest.json')); files = {}
            for m in t.getmembers():
                if m.isfile() and m.name != '_manifest.json': files[m.name] = t.extractfile(m).read()
    finally:
        try: os.remove(tgz)
        except Exception: pass
    import sys, threading
    stop = threading.Event(); _heartbeat(stop)
    try:
        return _do_ingest(cfg, man, files)
    finally: stop.set()

def _do_ingest(cfg, man, files):
    import sys
    return ingest(cfg, man['ids'], float(man.get('interval', 0.3)), float(man.get('latency_threshold', 1.0)), man.get('thumb_ids'),
                  progress=lambda m: (print('# ' + m), sys.stdout.flush()), images=split_images(files), backdrop_ids=man.get('backdrop_ids'),
                  files=files, thumb_files=man.get('thumb_files'))

def verify(cfg, ids):
    """{id: [流数, 有无Primary图, 有无背景图]},用于中断后核实"""
    return {str(i): list(item_state(cfg, i)) for i in ids}

# ---------------- 路径体检 ----------------
def check_paths(cfg):
    out = {}
    for key in ('library_db', 'strm_root', 'mediainfo_root'):
        p = cfg.get(key) or ''
        out[key] = {'path': p, 'exists': os.path.exists(p), 'readable': os.access(p, os.R_OK), 'writable': os.access(p, os.W_OK)}
    return out

if __name__ == '__main__':
    # 远程模式脚本入口: python3 hostlib.py <cfg.json> worklist | ingest <tgz> | check | info
    import sys
    cfg = json.load(open(sys.argv[1])); cmd = sys.argv[2]
    if cmd == 'worklist':
        rows, meta = build_worklist(cfg)
        for r in rows: print(json.dumps(r, ensure_ascii=False))
        print(json.dumps({'_meta': True, **meta}))
    elif cmd == 'ingest': print(json.dumps(ingest_tar(cfg, sys.argv[3])))
    elif cmd == 'verify': print(json.dumps(verify(cfg, json.load(open(sys.argv[3])))))
    elif cmd == 'check': print(json.dumps({'paths': check_paths(cfg), 'host': os.uname().nodename}))
    elif cmd == 'info': print(json.dumps(emby_info(cfg)))
