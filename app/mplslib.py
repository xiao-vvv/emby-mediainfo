import sys, struct, os, glob
def parse_mpls(b):
    if b[:4]!=b'MPLS': raise ValueError('not mpls')
    p=struct.unpack('>I',b[8:12])[0]
    p+=4+2; n_items=struct.unpack('>H',b[p:p+2])[0]; p+=4
    total=0.0; streams=[]
    for i in range(n_items):
        L=struct.unpack('>H',b[p:p+2])[0]; base=p+2; q=base+9
        flags=struct.unpack('>H',b[q:q+2])[0]; q+=2; multi=(flags>>4)&1
        q+=1; in_t,out_t=struct.unpack('>II',b[q:q+8]); q+=8; total+=(out_t-in_t)/45000.0
        q+=8+1+1+2
        if multi: na=b[q]; q+=2; q+=(na-1)*10
        s=q+2+2; c=list(b[s:s+7]); s+=7+5
        if i==0:
            groups=[('video',c[0]),('audio',c[1]),('pg',c[2]+c[6]),('ig',c[3]),('sec_audio',c[4]),('sec_video',c[5])]
            for kind,cnt in groups:
                for _ in range(cnt):
                    el=b[s]; e=s+1; st=b[e]
                    pid={1:e+1,2:e+3,3:e+2,4:e+3}.get(st)
                    pid=struct.unpack('>H',b[pid:pid+2])[0] if pid else None
                    s=e+el; al=b[s]; a=s+1; ct=b[a]; lang=None
                    if ct in (0x03,0x04,0x80,0x81,0x82,0x83,0x84,0x85,0x86,0xa1,0xa2): lang=b[a+2:a+5].decode('ascii','replace')
                    elif ct in (0x90,0x91): lang=b[a+1:a+4].decode('ascii','replace')
                    elif ct==0x92: lang=b[a+2:a+5].decode('ascii','replace')
                    s=a+al; streams.append((kind,pid,ct,lang))
        p=base+L
    return total, streams
