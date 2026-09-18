"""「Emby 主机」后端:同机模式在本进程执行 hostlib;远程模式通过 SSH 把 hostlib.py 推过去执行"""
import os, io, json, time, tarfile, shlex
import db, hostlib
from config import SECRETS

HERE = os.path.dirname(__file__)

PY = 'python3 -I'   # -I 隔离模式:不把脚本目录和用户 site-packages 放进 sys.path(远端以 root 跑,同目录放个 re.py 就会被 import)

def _last_json(out, what):
    """远端最后一行 JSON 就是结果;没有说明远端根本没跑到那步,把尾部输出带出来别抛裸 IndexError"""
    for l in reversed(out.split('\n')):
        if l.startswith('{'):
            try: return json.loads(l)
            except Exception: break
    raise RuntimeError(f'{what}: 远端没有返回结果,输出尾部: ' + out[-400:])

def host_cfg():
    s = db.all_settings()
    if s['mode'] == 'ssh':
        return {'emby_url': s['emby_url'], 'emby_api_key': s['emby_api_key'], 'emby_user_id': s['emby_user_id'],
                'library_db': s['remote_library_db'], 'emby_strm_prefix': s['emby_strm_prefix'], 'strm_root': s['remote_strm_root'], 'mediainfo_root': s['remote_mediainfo_root']}
    return {'emby_url': s['emby_url'], 'emby_api_key': s['emby_api_key'], 'emby_user_id': s['emby_user_id'],
            'library_db': s['library_db'], 'emby_strm_prefix': s['emby_strm_prefix'], 'strm_root': s['strm_root'], 'mediainfo_root': s['mediainfo_root']}

class LocalHost:
    name = '同机'
    def check(self):
        # ⚠️原来这里写死 ok=True:同机模式是默认模式,于是这个「体检」在结构上就不可能失败 ——
        #   路径一个都挂不上,页面照样显示「主机 OK」。ok 要由真实结果算出来
        paths = hostlib.check_paths(host_cfg())
        bad = [k for k, v in paths.items() if not v.get('exists')]
        # ⚠️媒体信息目录只读也必须算不健康:它「存在」但写不进去,而写不进去的后果是整批白刷一遍生产 Emby
        ro = [k for k, v in paths.items() if v.get('exists') and k == 'mediainfo_root' and not v.get('writable')]
        errs = (['这些路径在容器里看不到: ' + ', '.join(bad)] if bad else []) + (['媒体信息目录不可写: ' + ', '.join(ro)] if ro else [])
        return {'ok': not bad and not ro, 'host': os.uname().nodename, 'paths': paths,
                **({'err': ';'.join(errs)} if errs else {})}
    def emby_info(self): return hostlib.emby_info(host_cfg())
    def fetch_worklist(self): return hostlib.build_worklist({**host_cfg(), 'want_thumb': db.setting('capture_mode') != 'off'}, snapshot_dir='/config')
    def ingest(self, batch_id, files, ids, interval, thr, thumb_ids=None, backdrop_ids=None, thumb_files=None):
        cfg = host_cfg()
        return hostlib.ingest(cfg, ids, interval, thr, thumb_ids, images=hostlib.split_images(files), backdrop_ids=backdrop_ids,
                              files=files, thumb_files=thumb_files)
    def verify(self, ids): return hostlib.verify(host_cfg(), ids)

class SshHost:
    name = '远程 SSH'
    def _client(self):
        import paramiko
        s = db.all_settings()
        c = paramiko.SSHClient()
        # 🔴主机密钥必须校验:不校验的话,谁把 ssh_host 改成自己的机器,就能收走 cfg.json 里的 Emby api_key。
        #    首见即记(TOFU),之后主机密钥变了直接连不上。known_hosts 放在密钥旁边。
        # ⚠️私钥路径是个自由文本框,填成相对路径时 dirname 是空串 → makedirs('') 直接炸,SSH 整条路就废了
        kh = os.path.join(os.path.dirname(s.get('ssh_key') or '') or SECRETS, 'known_hosts')
        try:
            os.makedirs(os.path.dirname(kh), exist_ok=True)
            if not os.path.exists(kh): open(kh, 'a').close(); os.chmod(kh, 0o600)
            c.load_host_keys(kh)            # 必须成功:它同时把「写回哪个文件」告诉 paramiko,失败的话 AutoAdd 记下的密钥根本不落盘
        except Exception as e:
            # ⚠️原来这里是 except: pass —— 于是 known_hosts 一旦不可写(目录只读、文件属主不对),
            #   就会静默退化成「任何主机密钥都接受、而且永远不记住」,TOFU 等于没有,还一声不吭
            raise RuntimeError(f'known_hosts 不可用({kh}: {e}):主机密钥没法校验,不连。把这个目录改成可写再试')
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())      # 只对「没见过的主机」自动记;见过的变了会抛异常
        c.connect(s['ssh_host'], port=int(s['ssh_port']), username=s['ssh_user'], key_filename=s['ssh_key'], timeout=30, banner_timeout=30)
        try: c.save_host_keys(kh); os.chmod(kh, 0o600)
        except Exception: pass
        return c
    def _run(self, cmd, timeout=3600, idle_timeout=1800):
        """边读边等:同时收 stdout/stderr;exit-status 到了以后还要把通道排空到 EOF(数据包可能晚于 exit-status 到达);
        用 list+join 累积(bytes += 是二次方复杂度,待办清单有上百 MB);总超时 + 空闲超时"""
        import socket
        c = self._client()
        try:
            c.get_transport().set_keepalive(30)
            ch = c.get_transport().open_session(); ch.settimeout(5); ch.exec_command(cmd)
            o = []; e = []; t0 = time.time(); last = time.time()
            def drain(recv, buf):
                while True:
                    try: d = recv(65536)
                    except socket.timeout: return
                    if not d: return
                    buf.append(d)
            while True:
                got = False
                while ch.recv_ready(): o.append(ch.recv(65536)); got = True
                while ch.recv_stderr_ready(): e.append(ch.recv_stderr(65536)); got = True
                if got: last = time.time()
                if ch.exit_status_ready() and ch.eof_received:
                    drain(ch.recv, o); drain(ch.recv_stderr, e); break
                if time.time() - t0 > timeout: raise TimeoutError(f'远端命令超过 {timeout}s')
                if time.time() - last > idle_timeout: raise TimeoutError(f'远端命令 {idle_timeout}s 无输出')
                time.sleep(0.05)
            rc = ch.recv_exit_status()
            return rc, b''.join(o).decode('utf-8', 'replace'), b''.join(e).decode('utf-8', 'replace')
        finally: c.close()
    def _push(self, files_bytes, timeout=300):
        """{remote_path: bytes}
        🔴半开连接(NAT/防火墙悄悄把连接丢了)会让 write() 永远卡住,而 _push 是每个操作的第一步 ——
           卡住就意味着 feed_lock 永远不释放、条目永远停在 feeding、「立即入库」按钮永远是灰的,只能重启容器。
        ⚠️paramiko 这里有三个坑,少踩一个这道超时就形同虚设:
           ① `socket.timeout` 就是 `TimeoutError`,而 `TimeoutError ⊂ OSError ≡ IOError`。
             拿 `except IOError` 判「目录不存在」会把超时一起吞了,然后去 mkdir 一个本来就在的目录,
             超时预算还被乘三。所以只认 `FileNotFoundError`(paramiko 的 ENOENT 正好就是它)。
           ② `SFTPFile.close()` 内部 `except (IOError, socket.error): pass` —— 而写失败、配额、超时
             恰恰是在 close 那一刻才暴露的。所以推完必须自己核一遍体积,不能信「没抛异常」。
           ③ 不开 pipelined 的话是 32KB 一个来回:120MB 要 3840 次串行往返,链路一有延迟就慢得离谱。"""
        c = self._client()
        try:
            c.get_transport().set_keepalive(30)
            sf = c.open_sftp()
            sf.get_channel().settimeout(timeout)      # 每次读写的「无进展」上限,不是总时长:链路还在传就一直续命,彻底没动静才判死
            def exists(p):
                try: sf.stat(p); return True
                except FileNotFoundError: return False
            for path, data in files_bytes.items():
                d = os.path.dirname(path)
                if not exists(d):
                    cur = ''
                    for p in d.split('/'):
                        if not p: continue
                        cur += '/' + p
                        if not exists(cur): sf.mkdir(cur)
                with sf.open(path, 'wb') as f:
                    f.set_pipelined(True)
                    f.write(data)
                got = sf.stat(path).st_size
                if got != len(data): raise IOError(f'推送 {path} 不完整:远端 {got} 字节,应有 {len(data)} 字节')
                if path.endswith('cfg.json'):
                    try: sf.chmod(path, 0o600)      # 里面有 Emby api_key,别留 0644
                    except Exception: pass
            sf.close()
        finally: c.close()
    def _prep(self):
        s = db.all_settings(); rd = s['remote_dir'].rstrip('/')
        self._push({f'{rd}/hostlib.py': open(os.path.join(HERE, 'hostlib.py'), 'rb').read(),
                    f'{rd}/cfg.json': json.dumps({**host_cfg(), 'want_thumb': db.setting('capture_mode') != 'off'}).encode('utf-8')})
        return rd
    def check(self):
        try:
            rd = self._prep(); q = shlex.quote(rd); rc, o, e = self._run(f'{PY} {q}/hostlib.py {q}/cfg.json check', 60)
            if rc != 0: return {'ok': False, 'err': (e or o)[-300:]}
            return {'ok': True, **_last_json(o, 'check')}
        except Exception as ex: return {'ok': False, 'err': hostlib._clean(ex, host_cfg())[:300]}   # 别原样回,里面可能带着 cfg.json 的内容
    def emby_info(self):
        rd = self._prep(); q = shlex.quote(rd); rc, o, e = self._run(f'{PY} {q}/hostlib.py {q}/cfg.json info', 60)
        if rc != 0: raise RuntimeError((e or o)[-300:])
        return _last_json(o, 'info')
    def fetch_worklist(self):
        rd = self._prep(); q = shlex.quote(rd); rc, o, e = self._run(f'{PY} {q}/hostlib.py {q}/cfg.json worklist', 1800)   # ⚠️ 几十万行的清单会整块进内存(每 10 万行约 300MB),小内存机器要留意
        if rc != 0: raise RuntimeError('worklist 失败: ' + (e or o)[-500:])
        rows = [json.loads(l) for l in o.split('\n') if l.startswith('{')]
        meta = next((r for r in rows if r.get('_meta')), {})
        return [r for r in rows if not r.get('_meta')], meta
    def verify(self, ids):
        rd = self._prep(); name = f'verify-{os.getpid()}-{int(time.time()*1000)}.json'   # 固定文件名会被并发的另一次 verify 覆盖
        self._push({f'{rd}/{name}': json.dumps(ids).encode()})
        q = shlex.quote(rd); rc, o, e = self._run(f'{PY} {q}/hostlib.py {q}/cfg.json verify {q}/{name}; rc=$?; rm -f {q}/{name}; exit $rc', 3600)   # 直接接 rm 会把退出码换成 rm 的
        if rc != 0: raise RuntimeError('verify 失败: ' + (e or o)[-300:])
        return _last_json(o, 'verify')
    def ingest(self, batch_id, files, ids, interval, thr, thumb_ids=None, backdrop_ids=None, thumb_files=None):
        rd = self._prep(); buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode='w:gz') as t:
            for rel, content in files.items():
                b = content if isinstance(content, bytes) else content.encode('utf-8'); ti = tarfile.TarInfo(rel.lstrip('/')); ti.size = len(b); ti.mtime = int(time.time()); ti.mode = 0o644
                t.addfile(ti, io.BytesIO(b))
            m = json.dumps({'ids': ids, 'interval': interval, 'latency_threshold': thr, 'thumb_ids': thumb_ids or [], 'backdrop_ids': backdrop_ids or [],
                            'thumb_files': thumb_files or {}}).encode()
            ti = tarfile.TarInfo('_manifest.json'); ti.size = len(m); ti.mtime = int(time.time()); t.addfile(ti, io.BytesIO(m))
        remote = f'{rd}/inbox/{batch_id}.tgz'
        self._push({remote: buf.getvalue()})
        q = shlex.quote(rd)
        # 顺手清掉超过一天的残包:远端脚本没跑起来(解释器缺失、cfg 坏了、被 kill)时,那个 ≤120MB 的 tar 没人删
        clean = f'find {q}/inbox -maxdepth 1 -name "*.tgz" -mmin +1440 -delete 2>/dev/null;' if len(rd) > 1 else ''   # remote_dir 填成 / 的话这条会变成 find /inbox,别跑
        # 远端每几条打一次心跳(写 sidecar 每 25 个、预检每 20 条、传图每 5 条、刷新每 20 条),2400s 一点动静没有才算它死了。
        # ⚠️误判的代价不只是这一批白跑:本地会把整批退回 extracted,而远端其实还在跑,下一批等于在生产 Emby 上重刷一遍
        rc, o, e = self._run(f'{clean} {PY} {q}/hostlib.py {q}/cfg.json ingest {shlex.quote(remote)}', 7200, idle_timeout=2400)   # 远端每 20 条打心跳,1800s 无输出才算死
        if rc != 0: raise RuntimeError('ingest 失败: ' + (e or o)[-800:])
        return _last_json(o, 'ingest')

def get_host():
    return SshHost() if db.setting('mode') == 'ssh' else LocalHost()
