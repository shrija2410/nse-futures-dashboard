import csv,io,json,os,threading,time
from datetime import datetime
from urllib.request import Request,urlopen
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from zoneinfo import ZoneInfo

HOST='0.0.0.0'; PORT=int(os.environ.get('PORT','10000')); REFRESH=15; INSTR_REFRESH=3600
CID=os.environ.get('DHAN_CLIENT_ID','').strip(); TOKEN=os.environ.get('DHAN_ACCESS_TOKEN','').strip()
MASTER='https://images.dhan.co/api-data/api-scrip-master.csv'; LTP='https://api.dhan.co/v2/marketfeed/ltp'; IST=ZoneInfo('Asia/Kolkata')
state={'rows':[],'updated':'','error':'Starting…','busy':False,'symbols':0,'ok':0,'fail':0}; lock=threading.Lock()
inst=[]; inst_at=0; ilock=threading.Lock()

def now(): return datetime.now(IST)
def num(v):
    try:return float(str(v).replace(',','').strip()) if v not in (None,'') else None
    except:return None
def exp(v):
    try:return datetime.strptime(str(v)[:10],'%Y-%m-%d').date()
    except:return None

def get(url,timeout=60):
    r=Request(url,headers={'User-Agent':'Mozilla/5.0','Accept':'*/*'}); 
    with urlopen(r,timeout=timeout) as x:return x.read()

def dhan(payload):
    if not CID or not TOKEN: raise RuntimeError('Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN')
    r=Request(LTP,data=json.dumps(payload).encode(),headers={'Accept':'application/json','Content-Type':'application/json','access-token':TOKEN,'client-id':CID},method='POST')
    try:
        with urlopen(r,timeout=20) as x: raw=x.read(); status=getattr(x,'status',200)
    except Exception as e: raise RuntimeError(f'Dhan LTP request failed: {e}')
    if status!=200: raise RuntimeError(f'Dhan LTP HTTP {status}')
    try:o=json.loads(raw.decode())
    except:raise RuntimeError('Dhan returned invalid JSON')
    if str(o.get('status','')).lower() not in ('success','true',''): raise RuntimeError(f"Dhan API error: {o.get('message') or o.get('remarks') or o}")
    return o.get('data',{})

def load_inst(force=False):
    global inst,inst_at
    with ilock:
        if inst and not force and time.time()-inst_at<INSTR_REFRESH:return inst
        text=get(MASTER).decode('utf-8-sig','replace'); rd=csv.DictReader(io.StringIO(text)); today=now().date(); out=[]
        need={'SEM_EXM_EXCH_ID','SEM_SEGMENT','SEM_SMST_SECURITY_ID','SEM_INSTRUMENT_NAME','SEM_EXPIRY_DATE','SM_SYMBOL_NAME'}
        if not rd.fieldnames or not need.issubset(set(rd.fieldnames)):raise RuntimeError('Dhan instrument master format changed')
        for r in rd:
            if r.get('SEM_EXM_EXCH_ID','').strip()!='NSE':continue
            sym=r.get('SM_SYMBOL_NAME','').strip().upper(); sid=r.get('SEM_SMST_SECURITY_ID','').strip(); typ=r.get('SEM_INSTRUMENT_NAME','').strip().upper()
            if not sym or not sid:continue
            if typ=='EQUITY':out.append(('spot',sym,sid,None))
            elif typ=='FUTSTK':
                d=exp(r.get('SEM_EXPIRY_DATE'))
                if d and d>=today:out.append(('future',sym,sid,d))
        inst=out; inst_at=time.time(); return inst

def contracts():
    spots={}; fut={}
    for typ,sid_sym,sid,d in load_inst():
        if typ=='spot':spots.setdefault(sid_sym,sid)
        else:fut.setdefault(sid_sym,[]).append((d,sid))
    out={}
    for s,a in fut.items():
        a=sorted(a)
        if len(a)>=2:out[s]={'spot':spots.get(s),'near':a[0],'next':a[1],'far':a[2] if len(a)>2 else None}
    return out

def batches(a,n=1000):
    for i in range(0,len(a),n):yield a[i:i+n]

def quotes(cs):
    items=[]
    for s,c in cs.items():
        if c['spot']:items.append(('NSE_EQ',c['spot']))
        for k in ('near','next','far'):
            if c[k]:items.append(('NSE_FNO',c[k][1]))
    q={}
    for b in batches(items):
        p={}
        for seg,sid in b:p.setdefault(seg,[]).append(int(sid))
        data=dhan(p)
        for seg,vals in data.items():
            if isinstance(vals,dict):
                for sid,v in vals.items():
                    px=num(v.get('last_price')) if isinstance(v,dict) else None
                    if px is not None:q[(seg,str(sid))]=px
        time.sleep(1.05)
    return q

def make_rows(cs,q):
    rows=[]
    for s,c in cs.items():
        spot=q.get(('NSE_EQ',str(c['spot']))) if c['spot'] else None; near=q.get(('NSE_FNO',str(c['near'][1]))); nxt=q.get(('NSE_FNO',str(c['next'][1])))
        if spot is None or near is None or nxt is None:continue
        far=q.get(('NSE_FNO',str(c['far'][1]))) if c['far'] else None
        rows.append({'symbol':s,'spot':spot,'near':near,'next':nxt,'far':far,'spotNear':spot-near,'spotNext':spot-nxt,'nearNext':near-nxt,'nearExpiry':c['near'][0].isoformat(),'nextExpiry':c['next'][0].isoformat(),'farExpiry':c['far'][0].isoformat() if c['far'] else ''})
    return sorted(rows,key=lambda x:x['nearNext'])

def refresh():
    with lock:
        if state['busy']:return
        state['busy']=True; state['error']='Refreshing Dhan market data…'
    try:
        cs=contracts()
        if not cs:raise RuntimeError('No NSE stock-futures contracts found in Dhan instrument master')
        rows=make_rows(cs,quotes(cs))
        if not rows:raise RuntimeError('Dhan returned no usable stock-futures quotes. Check Dhan Data API access.')
        with lock:
            state.update(rows=rows,updated=now().strftime('%d-%b-%Y %H:%M:%S'),symbols=len(cs),ok=len(rows),fail=max(0,len(cs)-len(rows)),error='')
    except Exception as e:
        with lock:state['error']=f'{type(e).__name__}: {e}'
    finally:
        with lock:state['busy']=False

def worker():
    while True:refresh();time.sleep(REFRESH)

HTML='''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Live NSE Stock Futures Dashboard</title><style>body{font-family:Arial;margin:0;background:#f5f7fb;color:#162033}.wrap{max-width:1700px;margin:auto;padding:22px}.title{font-size:30px;font-weight:800}.sub{color:#64748b;margin:4px 0 18px}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.card{background:#fff;padding:16px;border-radius:12px}.label{font-size:13px;color:#64748b}.value{font-size:25px;font-weight:800;margin-top:5px}.bar{margin:14px 0;background:#fff;padding:12px;border-radius:12px;display:flex;gap:10px;align-items:center;flex-wrap:wrap}.bar input,.bar select,.bar button{padding:9px 11px;border:1px solid #cbd5e1;border-radius:8px;background:#fff}.bar button{background:#2563eb;color:white;border:0}.err{color:#c62828;font-weight:600}.ok{color:#16803c;font-weight:700}.tablewrap{background:#fff;border-radius:12px;overflow:auto}table{width:100%;border-collapse:collapse;min-width:1100px}th{position:sticky;top:0;background:#e9eef6;cursor:pointer}th,td{padding:10px 9px;border-bottom:1px solid #e5e7eb;text-align:right;white-space:nowrap}th:nth-child(1),td:nth-child(1),th:nth-child(2),td:nth-child(2){text-align:left}.pos{color:#16803c;font-weight:700}.neg{color:#c62828;font-weight:700}.qual{background:#ecfdf3}.small{font-size:12px;color:#64748b;margin-top:8px}@media(max-width:900px){.cards{grid-template-columns:repeat(2,1fr)}.title{font-size:24px}}</style></head><body><div class="wrap"><div class="title">Live NSE Stock Futures Dashboard</div><div class="sub">Dhan live market data • All NSE stock-futures • Spot − Near • Spot − Next • Near − Next</div><div class="cards"><div class="card"><div class="label">Stock Futures</div><div class="value" id="count">0</div></div><div class="card"><div class="label">Qualifying</div><div class="value" id="qual">0</div></div><div class="card"><div class="label">Lowest Near − Next</div><div class="value" id="low">—</div></div><div class="card"><div class="label">Last Update</div><div class="value" id="updated">—</div></div></div><div class="bar"><input id="search" placeholder="Search symbol" oninput="render()"><select id="filter" onchange="render()"><option value="all">All stocks</option><option value="qual">Qualifying: Spot &gt; Near &amp; Next + Near−Next &gt; 0</option><option value="positive">Near−Next &gt; 0</option></select><button onclick="load()">Refresh now</button><span id="status">Starting…</span></div><div class="tablewrap"><table><thead><tr><th>#</th><th>Symbol</th><th>Spot</th><th>Near</th><th>Next</th><th>Far</th><th>Spot−Near</th><th>Spot−Next</th><th>Near−Next</th></tr></thead><tbody id="body"></tbody></table></div><div class="small">Data source: DhanHQ Market Quote API. Prices should be verified before trading.</div></div><script>let data=[],sortKey='nearNext',asc=true;const n=x=>x==null?'—':Number(x).toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2});async function load(){try{let r=await fetch('/data?t='+Date.now()),j=await r.json();document.getElementById('count').textContent=j.symbols||data.length;document.getElementById('updated').textContent=j.updated||'—';if(j.error){document.getElementById('status').className='err';document.getElementById('status').textContent='Data status: '+j.error;if(j.rows&&j.rows.length)data=j.rows}else{data=j.rows||[];document.getElementById('status').className='ok';document.getElementById('status').textContent='Live • '+data.length+' futures loaded • '+(j.fail||0)+' unavailable • refresh ~15s'}render()}catch(e){document.getElementById('status').className='err';document.getElementById('status').textContent='Connection error: '+e}}function render(){let q=document.getElementById('search').value.toUpperCase(),f=document.getElementById('filter').value,rows=data.filter(x=>x.symbol.includes(q));if(f==='qual')rows=rows.filter(x=>x.spot>x.near&&x.spot>x.next&&x.nearNext>0);if(f==='positive')rows=rows.filter(x=>x.nearNext>0);rows.sort((a,b)=>((a[sortKey]??Infinity)-(b[sortKey]??Infinity))*(asc?1:-1));let qs=data.filter(x=>x.spot>x.near&&x.spot>x.next&&x.nearNext>0);document.getElementById('qual').textContent=qs.length;document.getElementById('low').textContent=qs.length?n(Math.min(...qs.map(x=>x.nearNext))):'—';document.getElementById('body').innerHTML=rows.map((x,i)=>{let q=x.spot>x.near&&x.spot>x.next&&x.nearNext>0;return `<tr class="${q?'qual':''}"><td>${i+1}</td><td><b>${x.symbol}</b></td><td>${n(x.spot)}</td><td>${n(x.near)}</td><td>${n(x.next)}</td><td>${n(x.far)}</td><td class="${x.spotNear>=0?'pos':'neg'}">${n(x.spotNear)}</td><td class="${x.spotNext>=0?'pos':'neg'}">${n(x.spotNext)}</td><td class="${x.nearNext>=0?'pos':'neg'}">${n(x.nearNext)}</td></tr>`}).join('')}document.querySelectorAll('th').forEach((th,i)=>th.onclick=()=>{let k=['rank','symbol','spot','near','next','far','spotNear','spotNext','nearNext'][i];if(k&&k!=='rank'){if(sortKey===k)asc=!asc;else{sortKey=k;asc=true}render()}})}load();setInterval(load,10000);</script></body></html>'''

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/healthz'):
            self.send_response(200);self.end_headers();self.wfile.write(b'ok');return
        if self.path.startswith('/data'):
            with lock:body=json.dumps(state,ensure_ascii=False).encode()
            self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(body);return
        self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.end_headers();self.wfile.write(HTML.encode())
    def log_message(self,*args):pass

if __name__=='__main__':
    print('LIVE NSE STOCK FUTURES DASHBOARD - DHAN V1');print(f'Refresh: {REFRESH}s')
    refresh();print('DATA ERROR:',state['error']) if state['error'] else print('Loaded',len(state['rows']),'rows')
    threading.Thread(target=worker,daemon=True).start();server=ThreadingHTTPServer((HOST,PORT),Handler);print('Listening on',PORT);server.serve_forever()
