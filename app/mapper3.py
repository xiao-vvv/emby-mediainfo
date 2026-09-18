# ffprobe 原始输出 -> Emby MediaSourceInfo 映射器 v3(规则来自对大量真实插件产出的统计,并用 100 份样本逐字段双向校验)
import os, math, struct
from fractions import Fraction

SUB_CODEC={'hdmv_pgs_subtitle':'PGSSUB','dvd_subtitle':'DVDSUB','dvb_subtitle':'DVBSUB'}
TEXT_SUBS={'subrip','ass','ssa','mov_text','webvtt','srt','text'}
AAC_DEFAULT={1:192000,2:192000,6:320000}
DOVI_DESC={'50':'Profile 5.0','81':'Profile 8.1 (HDR10 compatible)','76':'Profile 7.6 (Bluray)','84':'Profile 8.4 (HLG compatible)','82':'Profile 8.2 (SDR compatible)'}
KNOWN_AR_EXACT={Fraction(3,2):'1.5:1',Fraction(8,5):'1.6:1',Fraction(37,20):'1.85:1',Fraction(47,20):'2.35:1',Fraction(12,5):'2.40:1',Fraction(141,100):'1.41:1'}
KNOWN_AR=[(1.777777,'16:9',.02),(1.333333,'4:3',.02),(1.41,'1.41:1',.02),(1.5,'1.5:1',.02),(1.6,'1.6:1',.02),
          (1.6666667,'5:3',.02),(1.85,'1.85:1',.02),(2.35,'2.35:1',.025),(2.4,'2.40:1',.025)]

def f32s(x):
    """C# float 的最短往返表示"""
    if x is None: return None
    f=struct.unpack('f',struct.pack('f',x))[0]
    for p in range(1,10):
        s=f"{f:.{p}g}"
        if struct.unpack('f',struct.pack('f',float(s)))[0]==f: return float(s)
    return f
def frac(s):
    try:
        if not s or s in ('0/0','N/A'): return None
        v=Fraction(s); return None if v==0 else f32s(float(v))
    except Exception: return None
def real_fps(st):
    """r_frame_rate 恰为 avg 的 2 倍(mpeg1/vc1 场频)时 Emby 记的是 avg"""
    r=frac(st.get('r_frame_rate')); a=frac(st.get('avg_frame_rate'))
    if r and a and abs(r-2*a)<0.01: return a
    return r
# 冷门容器该填什么,是直接问线上 Emby 要的(拿它自己提取过的条目对):
#   .rmvb→rm  .wmv→asf  .mpg/.mpeg→mpeg  .ts→mpegts  .avi→avi  .flv→flv  .mov→mov  .m4v→mp4  .f4v→flv
# 也就是说 Emby 用的是「规范化的容器名」,不是文件扩展名(否则 .rmvb 该是 rmvb、.wmv 该是 wmv)。
# ⚠️ffprobe 把 .f4v 报成 mov,mp4,m4a,3gp,3g2,mj2,但 Emby 认它是 flv —— 这条只能靠查表
EXT_CONTAINER={'.f4v':'flv'}
def container(fmt,path):
    n=(fmt.get('format_name') or ''); ext=os.path.splitext(path or '')[1].lower()
    if ext in EXT_CONTAINER: return EXT_CONTAINER[ext]
    if n.startswith('matroska'): return 'webm' if ext=='.webm' else 'mkv'
    if n.startswith('mov,mp4'): return 'mov' if ext=='.mov' else 'mp4'
    return n.split(',')[0]
def bitdepth_video(pix):
    if not pix: return None
    for d in ('16','14','12','10','9'):
        if (d+'le') in pix or (d+'be') in pix: return int(d)
    if pix.startswith(('rgb48','rgba64','bgr48','bgra64')): return 16
    return 8
def bitdepth_audio(st):
    for k in ('bits_per_raw_sample','bits_per_sample'):
        try:
            v=int(st.get(k) or 0)
            if v: return v
        except Exception: pass
    return None
def aspect(st):
    """统计自神医 json:有 DAR 用 DAR,没有用 W×SAR:H;两者都先查精确比例表(3:2→1.5:1, 12:5→2.40:1 ...),不命中则约分原样"""
    dar=st.get('display_aspect_ratio')
    if dar and dar not in ('0:1','N/A'):
        try: a,b=dar.split(':'); fr=Fraction(int(a),int(b))
        except Exception: return dar
    else:
        w,h=st.get('width'),st.get('height')
        if not w or not h: return None
        num,den=w,h; sar=st.get('sample_aspect_ratio')
        if sar and ':' in sar and sar not in ('0:1','N/A'):
            try:
                a,b=sar.split(':')
                if int(a) > 0 and int(b) > 0: num*=int(a); den*=int(b)
            except (TypeError, ValueError): pass
        try: fr=Fraction(num,den)
        except (ZeroDivisionError, TypeError, ValueError): return None
    if fr in KNOWN_AR_EXACT: return KNOWN_AR_EXACT[fr]
    return f"{fr.numerator}:{fr.denominator}"
def chan_layout(s): return s.split('(')[0] if s else None
def video_range(st):
    for sd in (st.get('side_data_list') or []):
        if isinstance(sd, dict) and sd.get('side_data_type')=='DOVI configuration record': return 'DolbyVision',sd
    ct=st.get('color_transfer')
    if ct=='smpte2084': return 'HDR 10',None
    if ct=='arib-std-b67': return 'HLG',None
    return 'SDR',None
def ext_video(st):
    vr,dovi=video_range(st)
    if vr=='DolbyVision':
        p=dovi.get('dv_profile'); c=dovi.get('dv_bl_signal_compatibility_id')
        if p is None: return 'DolbyVision','DolbyVision','Dolby Vision'      # 记录里没有 profile 就别拼出 DoviProfileNone 这种鬼东西
        key=f"{p}{c if c is not None else ''}"
        return 'DolbyVision',f'DoviProfile{key}',DOVI_DESC.get(key,f'Profile {p}.{c}' if c is not None else f'Profile {p}')
    if vr=='HDR 10': return 'Hdr10','Hdr10','HDR 10'
    if vr=='HLG': return 'HyperLogGamma','HyperLogGamma','HLG'
    return 'None','None','None'
def int_or_none(v):
    try: return int(v) if v not in (None,'','N/A') else None
    except Exception: return None

def map_probe(pr,path='',is_bluray=False):
    fmt=pr.get('format',{}) or {}
    size=int_or_none(fmt.get('size')); dur=duration_of(pr)
    ticks=int(round(dur*1e7)) if dur else None
    bitrate=int(size*8*1e7/ticks) if size and ticks else int_or_none(fmt.get('bit_rate'))
    # 🔴钳在 Int32 内:Emby/Jellyfin 的 Bitrate 是 int?,超界会让整份 sidecar 反序列化失败。
    #   触发路径是真实存在的:40GB 的 ISO 要是把主播放列表挑成 30 秒的片头(第五轮踩过),
    #   推出来的码率就是 106 亿。宁可不写这个字段,也不能写一个会让 Emby 解析不了的值
    if bitrate is not None and not (0 < bitrate <= 2**31 - 1): bitrate=None
    out={'Container':'iso' if is_bluray else container(fmt,path),'Size':size,'Bitrate':bitrate,
         'RunTimeTicks':ticks,'Protocol':'File','Type':'Default',
         'IsRemote':True,'AddApiKeyToDirectStreamUrl':False,'SupportsTranscoding':True,'SupportsDirectStream':True,'SupportsDirectPlay':True,
         'SupportsProbing':True,'IsInfiniteStream':False,'RequiresOpening':False,'RequiresClosing':False,
         'RequiresLooping':False,'ReadAtNativeFramerate':False,'HasMixedProtocols':False,
         'RequiredHttpHeaders':{},'Formats':[],'Chapters':[],'MediaStreams':[]}
    vids=[s for s in (pr.get('streams') or []) if s.get('codec_type')=='video' and not (s.get('disposition') or {}).get('attached_pic')]
    p7_el = is_bluray and len(vids)>=2 and any(s.get('id')=='0x1015' for s in vids)
    for st in (pr.get('streams') or []):
        t=st.get('codec_type'); disp=st.get('disposition') or {}
        tags={k.lower():v for k,v in (st.get('tags') or {}).items()}
        if t=='video' and disp.get('attached_pic'): continue
        s={'Index':st.get('index'),'Codec':st.get('codec_name'),'IsDefault':bool(disp.get('default')),
           'IsForced':bool(disp.get('forced')),'IsHearingImpaired':bool(disp.get('hearing_impaired')),
           'Title':tags.get('title'),'Language':tags.get('language'),'Protocol':'File','TimeBase':st.get('time_base'),
           'IsExternal':False,'IsInterlaced':False,'ExtendedVideoType':'None','ExtendedVideoSubType':'None',
           'ExtendedVideoSubTypeDescription':'None','IsTextSubtitleStream':False,'SupportsExternalStream':False,
           'BitRate':int_or_none(st.get('bit_rate')),'AttachmentSize':0}
        if t=='video':
            vr,_=video_range(st); et,est,desc=ext_video(st)
            if p7_el and st.get('id')=='0x1015': vr,et,est,desc='DolbyVision','DolbyVision','DoviProfile76',DOVI_DESC['76']
            s.update({'Type':'Video','Width':st.get('width'),'Height':st.get('height'),'Profile':st.get('profile'),
                      'Level':st.get('level'),'PixelFormat':st.get('pix_fmt'),'BitDepth':bitdepth_video(st.get('pix_fmt')),
                      'ColorPrimaries':st.get('color_primaries'),'ColorSpace':st.get('color_space'),'ColorTransfer':st.get('color_transfer'),
                      'VideoRange':vr,'ExtendedVideoType':et,'ExtendedVideoSubType':est,'ExtendedVideoSubTypeDescription':desc,
                      'AverageFrameRate':frac(st.get('avg_frame_rate')),'RealFrameRate':real_fps(st),
                      'RefFrames':st.get('refs'),'NalLengthSize':st.get('nal_length_size'),
                      'IsInterlaced':st.get('field_order') not in (None,'progressive','unknown'),
                      'AspectRatio':aspect(st),'IsAnamorphic':st.get('sample_aspect_ratio') not in (None,'1:1','0:1','N/A'),
                      'BitRate':int_or_none(st.get('bit_rate')) or bitrate})
        elif t=='audio':
            br=int_or_none(st.get('bit_rate'))
            if br is None and st.get('codec_name')=='aac': br=AAC_DEFAULT.get(st.get('channels'))
            s.update({'Type':'Audio','Channels':st.get('channels'),'ChannelLayout':chan_layout(st.get('channel_layout')),
                      'SampleRate':int_or_none(st.get('sample_rate')),'Profile':st.get('profile'),'BitDepth':bitdepth_audio(st),'BitRate':br})
        elif t=='subtitle':
            c=st.get('codec_name'); s['Codec']=SUB_CODEC.get(c,c)
            s.update({'Type':'Subtitle','SubtitleLocationType':'InternalStream','IsTextSubtitleStream':c in TEXT_SUBS,'SupportsExternalStream':c in TEXT_SUBS,'BitRate':int_or_none(st.get('bit_rate'))})
            if st.get('width'): s['Width']=st.get('width'); s['Height']=st.get('height')
        elif t=='attachment':
            fn=tags.get('filename') or ''
            s.update({'Type':'Attachment','Codec':os.path.splitext(fn)[1].lstrip('.').lower() or None,'Path':fn,'BitRate':None,'MimeType':tags.get('mimetype'),'AttachmentSize':int_or_none(st.get('extradata_size')) or 0})
        else: continue
        out['MediaStreams'].append(s)
    return out


def chapters(pr):
    """真章节:tags.title/毫秒取整;文件无章节:Emby 每 300 秒生成一个假章节(个数=时长//300+1)"""
    out=[]; ch=pr.get('chapters') or []
    if ch:
        # 上限和假章节那条一致:文件自己声明几万个章节也得拦(几十万个文件里什么都会有)
        for i,c in enumerate(ch[:2000]):
            try: st=float(c.get('start_time',0) or 0)
            except (TypeError, ValueError): st=0.0
            # nan/inf 能通过 float() 但会在 int(round(...)) 上抛;负数是无意义的起点
            if st != st or st in (float('inf'), float('-inf')) or st < 0: st=0.0
            title=(c.get('tags') or {}).get('title')
            # ⚠️这里必须用 round()(银行家舍入):C# 的 Math.Round 默认就是它,改成四舍五入会和神医差 1ms(100 份语料对账抓到过)
            ticks=int(round(st*1000))*10000
            if ticks > 2**63 - 1: ticks=0                    # Int64 上界:start_time=1e30 这种会让 C# 反序列化直接失败
            out.append({'StartPositionTicks':ticks,'Name':title if title else f'章节 {i+1}','MarkerType':'Chapter','ChapterIndex':i})
        return out
    dur=duration_of(pr) or 0                                     # 用和媒体信息同一套时长回退,否则 mpegts/ISO 这类会一个假章节都没有
    n=int(dur//300)+1 if dur and dur > 0 else 0
    if n > 2000: n=0                                             # 时长离谱(损坏文件报几百天)→ 一个都别生成,免得几十万个章节把内存吃光
    for i in range(n):
        out.append({'StartPositionTicks':i*300*10_000_000,'Name':f'章节 {i+1}','MarkerType':'Chapter','ChapterIndex':i})
    return out
def duration_of(pr):
    """容器时长;没有就退回流时长 / mkv 的 DURATION 标签。负数和离谱值一律当没有"""
    fmt=pr.get('format',{}) or {}
    dur=None
    try:
        v=float(fmt.get('duration') or 0)
        if 0 < v <= 30*86400: dur=v                               # 负数/超过 30 天的容器时长当没给,继续看流时长
    except (TypeError, ValueError): pass
    if not dur:
        for st in pr.get('streams',[]) or []:
            try:
                d=float(st.get('duration') or 0)
            except (TypeError, ValueError): d=0.0
            if not d:
                try:
                    t=(st.get('tags') or {}).get('DURATION') or (st.get('tags') or {}).get('DURATION-eng')
                    if t: h,m_,s_=t.split(':'); d=int(h)*3600+int(m_)*60+float(s_)
                except Exception: d=0.0
            if d > 0: dur=max(dur or 0.0, d)
    if dur and (dur <= 0 or dur > 30*86400): return None          # 流时长也离谱 → 当没拿到(上层会判失败,总比写个 200 小时进库强)
    return dur

def _strip(o):
    if isinstance(o,dict): return {k:_strip(v) for k,v in o.items() if v is not None}
    if isinstance(o,list): return [_strip(x) for x in o]
    return o
def build_sidecar(pr,path='',is_bluray=False):
    """神医 -mediainfo.json 的完整结构:[{"MediaSourceInfo":{...},"Chapters":[...]}]"""
    return _strip([{'MediaSourceInfo':map_probe(pr,path,is_bluray),'Chapters':chapters(pr)}])
