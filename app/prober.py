"""ffprobe:HTTP 直链 / ISO(bluray: + MPLS 语言)"""
import subprocess, json, os, glob, shutil, tempfile, sys, time
sys.path.insert(0, os.path.dirname(__file__))
import mapper3, mplslib
FFPROBE = os.environ.get('FFPROBE', 'ffprobe'); FFMPEG = os.environ.get('FFMPEG', 'ffmpeg'); SEVENZ = os.environ.get('SEVENZ', '7zz')
ARGS = ['-threads', '0', '-v', 'error', '-print_format', 'json', '-show_streams', '-show_chapters', '-show_format']

import re
def _clean_err(stderr, cmd):
    """把 stderr 里的直链 URL 抹掉,只留真正的错误原因(403/404/超时/解码错误)"""
    s = stderr.decode('utf-8', 'replace')
    for a in cmd:
        if isinstance(a, str) and a.startswith('http'): s = s.replace(a, '<url>')
    s = re.sub(r'https?://\S+', '<url>', s)
    lines = [l.strip() for l in s.splitlines() if l.strip() and not l.startswith('  ')]
    return ' | '.join(lines[-3:])[-300:]
def run(cmd, timeout):
    r = subprocess.run(cmd, capture_output=True, timeout=timeout)
    if r.returncode != 0 or not r.stdout.strip():
        raise RuntimeError('ffprobe rc=%d %s' % (r.returncode, _clean_err(r.stderr, cmd)))
    pr = json.loads(r.stdout.decode('utf-8', 'replace'))
    if not pr.get('streams'): raise RuntimeError('ffprobe 无 streams')
    return pr

def probe_http(url, ua, timeout):
    # -multiple_requests 1:一条文件要 50+ 次分段读,复用同一条连接,少给 115 CDN 触发限流的机会
    return run([FFPROBE, '-user_agent', ua, '-multiple_requests', '1'] + ARGS + ['-i', url], timeout)

def probe_iso(local_path, timeout):
    """蓝光 ISO:libbluray 探主标题 + 7zz 抽 MPLS 按时长匹配 → 语言按 PID 回填"""
    pr = run([FFPROBE] + ARGS + ['-i', 'bluray:' + local_path], timeout)
    dur = float(pr['format'].get('duration') or 0)
    tmp = tempfile.mkdtemp(prefix='mpls_')
    try:
        # ⚠️MPLS 只是「顺便把语言补全」,7zz 缺了/超时/解不开都不该把上面那份已经成功的 ffprobe 结果一起丢掉
        try:
            r7 = subprocess.run([SEVENZ, 'e', '-y', '-o' + tmp, local_path, 'BDMV/PLAYLIST/*'],
                                capture_output=True, timeout=min(300, max(30, timeout // 2)))
            if r7.returncode != 0:
                # ⚠️返回码不看的话,ISO 尾部损坏时 7zz 抽出前几个 playlist 就 exit 2,
                #   那批**残缺**的 playlist 照样参与时长匹配和语言回填,而且一个字都不会记
                print(f'[warn] 7zz 退出码 {r7.returncode},MPLS 可能不全,语言不回填: '
                      f'{(r7.stderr or b"")[-160:].decode("utf-8", "replace")}', flush=True)
                return pr
        except Exception as e:
            print(f'[warn] 7zz 抽 MPLS 失败(语言不回填,媒体信息照常): {str(e)[:120]}', flush=True)
            return pr
        best = None
        # ⚠️要排序:两条 playlist 时长完全相同时(无缝分支/导演剪辑/防盗版混淆),
        #   原来是「glob 返回的第一个」获胜,而 glob 不排序 —— 同一张碟每次跑可能选到不同的音轨语言
        for f in sorted(glob.glob(tmp + '/*.mpls') + glob.glob(tmp + '/*.MPLS')):     # 区分大小写的文件系统上大写的那批会被漏掉
            try:
                with open(f, 'rb') as fh: d, st = mplslib.parse_mpls(fh.read())
            except Exception: continue
            if dur > 0:
                if best is None or abs(d - dur) < abs(best[0] - dur): best = (d, st)
            elif best is None or d > best[0]: best = (d, st)     # 🔴没有容器时长就取最长的正片,别按「和 0 最接近」挑——那会挑到几秒的片头 logo
        if best and ((dur > 0 and abs(best[0] - dur) < 5) or (dur <= 0 and best[0] > 600)):
            pids = {p: l for k, p, c, l in best[1] if l and len(l) == 3 and l.isascii() and l.isalpha()}   # 全 0/乱码的语言码别拿来覆盖 ffprobe 的
            for s in pr['streams']:
                try: pid = int(s.get('id', '0x0'), 16)
                except Exception: continue
                if pid in pids and s.get('codec_type') in ('audio', 'subtitle'):
                    s.setdefault('tags', {})['language'] = pids[pid]
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return pr

def sidecar(pr, rel115, is_iso=False):
    return mapper3.build_sidecar(pr, rel115, is_bluray=is_iso)

# HDR→SDR:镜像 ffmpeg 没有 zscale,用 libplacebo(需要 Vulkan 设备;没有 GPU 时用 lavapipe CPU 实现,4K 一帧约 8 秒)
# 与神医助手实际命令一致:hwupload,libplacebo=format=yuv420p:colorspace=bt709:color_primaries=bt709:color_trc=bt709,hwdownload(色调映射算法用 libplacebo 默认)
FRAMES = int(os.environ.get('CAPTURE_FRAMES', '24') or 24)   # Emby 原生就是在那一秒里抓 24 帧挑一张
HDR_VF = os.environ.get('CAPTURE_HDR_VF', 'hwupload,libplacebo=format=yuv420p:colorspace=bt709:color_primaries=bt709:color_trc=bt709,hwdownload,format=yuv420p')
HDR_EQ = os.environ.get('CAPTURE_HDR_EQ', '')   # 默认不加后处理,与神医产出观感一致;想更通透可设 eq=contrast=1.05:saturation=1.05
VK_ICD = os.environ.get('VK_ICD_FILENAMES') or ('/usr/share/vulkan/icd.d/lvp_icd.x86_64.json' if os.path.exists('/usr/share/vulkan/icd.d/lvp_icd.x86_64.json') else '')
def is_hdr(pr):
    v = next((s for s in pr.get('streams', []) if s.get('codec_type') == 'video' and not (s.get('disposition') or {}).get('attached_pic')), None)
    if not v: return False
    return v.get('color_transfer') in ('smpte2084', 'arib-std-b67') or any('DOVI' in (sd.get('side_data_type') or '') for sd in (v.get('side_data_list') or []))
def _ffmpeg_cap(src, pos, vf, out_path, ua, quality, timeout, vulkan=False, frames=1, check_size=True, _lean=True):
    # ⭐截图之前已经跑过一次完整的 ffprobe 了,流结构早就拿到手 —— 这里再做一遍默认 5MB 的流分析纯属重复读。
    #   实测同一文件少读 242KB、输出的 JPEG 逐字节相同。
    #   ⚠️配一个退路:万一某种容器前 200KB 认不出视频流,自动退回默认参数再来一次(别为了省这点把整类文件截不出来)
    cmd = [FFMPEG, '-y', '-v', 'error']
    if _lean: cmd += ['-probesize', '200k', '-analyzeduration', '0']
    if vulkan: cmd += ['-init_hw_device', 'vulkan=vk', '-filter_hw_device', 'vk']
    if ua and src.startswith('http'): cmd += ['-user_agent', ua, '-multiple_requests', '1']
    # 🔴 -map 0:V:0 —— 大写 V 表示「排除内嵌封面(attached_pic)的视频流」。
    #    不指定的话 ffmpeg 按分辨率挑「最大的那条视频流」:一个 360p 的老剧配一张 1000x1500 的内嵌封面,
    #    截出来的就是那张封面被裁成 16:9 的一块,而不是剧本身。神医专门魔改 Emby 跳过内嵌图,就是这件事
    cmd += ['-ss', f'{pos:.3f}', '-i', src, '-map', '0:V:0', '-an', '-sn', '-frames:v', str(int(frames)), '-vf', vf, '-q:v', str(int(quality)), '-f', 'image2', out_path]
    env = dict(os.environ)
    if vulkan and VK_ICD: env['VK_ICD_FILENAMES'] = VK_ICD
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=timeout, env=env, stdin=subprocess.DEVNULL)
    except Exception:
        try: os.remove(out_path)                       # 超时抛在下面的检查之前,半张图会留下来顶掉原来那张好的
        except Exception: pass
        raise
    if r.returncode != 0 or (check_size and (not os.path.exists(out_path) or os.path.getsize(out_path) < 1000)):
        if check_size:
            try: os.remove(out_path)                   # 失败会留下半张图,别让它被当成成品发出去
            except Exception: pass
        why = _clean_err(r.stderr, cmd)
        if _lean and r.returncode != 0:
            # 省读那套参数没认出流 → 退回默认参数重来一次
            return _ffmpeg_cap(src, pos, vf, out_path, ua, quality, timeout, vulkan=vulkan, frames=frames, check_size=check_size, _lean=False)
        if r.returncode == 0 and not why:      # ffmpeg 说成功,只是图太小(纯黑/低信息量的一帧) —— 别报成「rc=0」让人以为崩了
            sz = os.path.getsize(out_path) if os.path.exists(out_path) else 0
            why = f'输出只有 {sz} 字节(基本是纯黑/无内容的一帧)'
        raise RuntimeError('rc=%d %s' % (r.returncode, why))
def brightness(path, timeout=60):
    """这张图的平均亮度(0~255),取不到回 -1。用来判断是不是撞上了暗场/黑场"""
    try:
        r = subprocess.run([FFMPEG, '-v', 'error', '-i', path, '-vf', 'signalstats,metadata=print:key=lavfi.signalstats.YAVG:file=-',
                            '-f', 'null', '-'], capture_output=True, text=True, timeout=timeout, stdin=subprocess.DEVNULL)   # ⚠️不写 file=- 就什么都不输出
        for line in (r.stderr + r.stdout).splitlines():
            if 'YAVG' in line:
                try: return float(line.rsplit('=', 1)[-1].strip())
                except ValueError: pass
    except Exception: pass
    return -1.0
def capture(src, pr, out_path, ua=None, position_pct=20, tonemap=True, quality=2, timeout=300, pick_best=True, avoid_dark=True):
    """在 position% 处取一帧 → (HDR 则色调映射) → 16:9 裁切 → jpg(神医 EnsureAspect16By9 同款)。src 可为 HTTP 直链或本地文件
    pick_best:先在那一秒里抓 24 帧小图,挑**最静止**的那一帧,再按它的时间点正式截。
              这就是 Emby 原生(神医直接用原生逻辑)的做法 —— 单帧硬抓容易撞上运动模糊。
              ⚠️别改成按「最锐利」挑:试过,盲评更差(运镜中的特写最锐利,恰恰是该避开的)
    avoid_dark:挑出来的画面太黑(夜戏/转场黑场)就往后再试两个位置。⚠️这一步神医/原生没有,是我们多做的,默认关"""
    dur = float(pr.get('format', {}).get('duration') or 0)
    pct = min(95.0, max(1.0, float(position_pct)))
    crop = 'crop=iw:min(ih\\,iw*9/16)'
    positions = [pct]
    if avoid_dark and dur > 60: positions += [p for p in (pct + 20, pct + 40) if p <= 90]
    err = ''; last = None; best = (-1.0, None)          # 换位置重试时要留「最亮的那张」,不是最后一张
    deadline = time.time() + timeout                    # ⭐总预算:每个子进程各自限时是不够的(三个位置 × HDR 两次 × 打分两次能跑到 6 倍)
    def keep_best():
        if best[1] and os.path.exists(best[1]):
            try: shutil.move(best[1], out_path)
            except Exception: pass
    def drop_cand():
        if best[1] and os.path.exists(best[1]):
            try: os.remove(best[1])
            except Exception: pass
    def _done():
        # 🔴每条返回路径都得确认文件真的产出来了。踩过:位置 0 失败且把预算耗光 → 下一轮开头「预算不够了,
        #    手上这张先用着」→ 可手上根本没有这张,返回一个不存在的路径,上游照样记成「截图成功」,
        #    等到下一批入库读文件才炸,白烧一次重试和一次 115 直链
        if not os.path.exists(out_path):
            raise RuntimeError('截图没产出文件(预算耗尽或全部位置都失败)' + (': ' + err[:120] if err else ''))
        return out_path
    for k, p in enumerate(positions):
        left = deadline - time.time()
        if k and left < 10:                             # 预算快用完了就别再换位置了,手上这张先用着
            if best[1] and os.path.exists(best[1]): keep_best()
            return _done()
        pos = max(1.0, min(dur - 1.0, dur * p / 100.0)) if dur > 2 else 1.0   # 没时长就取第 1 秒,别盲跳 60 秒(短片会直接失败)
        def shoot(vf, tmo, vk=False):
            tmo = max(5, min(tmo, int(deadline - time.time())))    # 子进程的时限也要收在总预算里(下限 5 秒,免得给出 0)
            # ⭐挑帧用 ffmpeg 自带的 thumbnail 滤镜:在这 24 帧里挑最有代表性的一张,**一次解码、一次下载**。
            #   放在滤镜链最前面 —— 只有选中的那一帧才往后走,HDR 也就只过一次 libplacebo。
            #   ⚠️自己实现「先下小图打分、再回头正式截」会把同一段数据下两遍:本地看不出差别(0.45s vs 0.43s),
            #     但线上 115 只给 3 条并发,实测速率从 90~120 条/分掉到 35~43 条/分
            _ffmpeg_cap(src, pos, (f'thumbnail=n={FRAMES},' if pick_best and dur > 3 else '') + vf,
                        out_path, ua, quality, tmo, vulkan=vk)
        try:
            if tonemap and is_hdr(pr):
                try:
                    shoot(','.join(x for x in (HDR_VF, HDR_EQ, crop) if x), max(30, int(timeout * 0.6)), vk=True)
                except Exception as e:
                    err = str(e)   # 色调映射失败 → 退回不映射(画面发灰但可用)
                    shoot(crop, max(30, int(timeout * 0.4)))   # 只给剩下的四成:否则一个 HDR 文件能占着 115 槽位十几分钟
            else:
                shoot(crop, timeout)
        except Exception as e:
            last = e
            if k == len(positions) - 1:
                if best[1] and os.path.exists(best[1]): keep_best(); return _done()   # 后面几次都失败了,至少把之前那张留下
                raise RuntimeError('截图失败 ' + (err + ' | ' if err else '') + str(e))
            continue
        if not avoid_dark: return _done()
        y = brightness(out_path, timeout=max(2, min(60, int(deadline - time.time()))))   # 亮度检测也要收在总预算里
        if y < 0 or y >= 26: drop_cand(); return _done()         # 取不到亮度就认了;26/255 以下基本是黑场或纯夜戏
        if k == len(positions) - 1:                               # 几个位置都偏暗 → 挑其中最亮的
            if best[0] > y: keep_best()
            else: drop_cand()
            return _done()
        if y > best[0]:                                           # 记下目前最亮的那张,继续换位置试
            drop_cand(); cand = out_path + '.cand'
            try: shutil.copy(out_path, cand); best = (y, cand)
            except Exception: best = (-1.0, None)
    if last: raise RuntimeError('截图失败 ' + str(last))
    return _done()
