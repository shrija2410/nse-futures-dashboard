import json, threading, time, webbrowser, traceback, concurrent.futures, os
from datetime import datetime
from urllib.request import Request, build_opener
from http.cookiejar import CookieJar
from urllib.parse import urlencode, quote
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOST='0.0.0.0'; PORT=int(os.environ.get('PORT','10000'))
# NSE protects its public endpoints aggressively. 30s is a safer live-refresh interval
# than hammering 200+ symbols every 15s.
REFRESH=30
MAX_WORKERS=8
NSE='https://www.nseindia.com'
UNDERLYING_URL=NSE+'/api/underlying-information'
DERIV_URL=NSE+'/api/NextApi/apiClient/GetQuoteApi'
BOOT_PAGES=[NSE+'/', NSE+'/market-data/live-equity-market']
UA='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'

class NSEHTTPError(Exception): pass

class NSEClient:
    def __init__(self):
        self.jar=CookieJar()
        self.opener=build_opener(__import__('urllib.request',fromlist=['HTTPRedirectHandler']).HTTPRedirectHandler(), __import__('urllib.request',fromlist=['HTTPHandler']).HTTPHandler(), __import__('urllib.request',fromlist=['HTTPSHandler']).HTTPSHandler(), __import__('urllib.request',fromlist=['HTTPErrorProcessor']).HTTPErrorProcessor(), __import__('urllib.request',fromlist=['HTTPCookieProcessor']).HTTPCookieProcessor(self.jar))
        self.lock=threading.RLock()
        self.last_boot=0

    def _headers(self, accept_json=True, referer=NSE+'/'):
        h={'User-Agent':UA,'Accept-Language':'en-US,en;q=0.9,hi;q=0.8','Referer':referer,'Connection':'keep-alive','Cache-Control':'no-cache','Pragma':'no-cache'}
        if accept_json:
            h['Accept']='application/json, text/plain, */*'
        else:
            h['Accept']='text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
        # Deliberately omit Accept-Encoding. NSE may return Brotli otherwise.
        return h

    def get_json(self,url,referer=NSE+'/',timeout=12):
        req=Request(url,headers=self._headers(True,referer),method='GET')
        try:
            with self.opener.open(req,timeout=timeout) as r:
                status=getattr(r,'status',200)
                raw=r.read()
                if status != 200: raise NSEHTTPError(f'HTTP {status}')
        except Exception as e:
            raise
        text=raw.decode('utf-8-sig',errors='replace')
        if text.lstrip().startswith('<'):
            raise NSEHTTPError('NSE returned an HTML page instead of JSON (likely bot protection/session issue).')
        try: return json.loads(text)
        except Exception: raise NSEHTTPError('NSE returned invalid JSON.')

    def bootstrap(self, force=False):
        with self.lock:
            if not force and time.time()-self.last_boot < 240 and len(self.jar):
                return
            for page in BOOT_PAGES:
                req=Request(page,headers=self._headers(False,NSE+'/'),method='GET')
                try:
                    with self.opener.open(req,timeout=15) as r: r.read(2048)
                except Exception:
                    pass
            self.last_boot=time.time()

    def symbols(self):
        self.bootstrap()
        payload=self.get_json(UNDERLYING_URL,NSE+'/market-data/equity-derivatives')
        data=payload.get('data',payload) if isinstance(payload,dict) else {}
        lst=data.get('UnderlyingList',[]) if isinstance(data,dict) else []
        out=[]
        for x in lst:
            if isinstance(x,dict):
                s=x.get('symbol') or x.get('Symbol')
            else: s=x
            if s: out.append(str(s).strip())
        return sorted(set(out))

    def derivatives(self,symbol):
        self.bootstrap()
        url=DERIV_URL+'?'+urlencode({'functionName':'getSymbolDerivativesData','symbol':symbol})
        try:
            return self.get_json(url,NSE+'/get-quotes/derivatives?symbol='+quote(symbol,safe=''))
        except Exception as e:
            # Refresh cookies once if NSE has invalidated the session.
            msg=str(e)
            if '403' in msg or '401' in msg or 'HTML' in msg:
                self.bootstrap(force=True)
                return self.get_json(url,NSE+'/get-quotes/derivatives?symbol='+quote(symbol,safe=''))
            raise


def num(v):
    try:
        if v is None or v=='': return None
        return float(str(v).replace(',','').replace('%','').strip())
    except: return None

def pick(d,*keys):
    for k in keys:
        if isinstance(d,dict) and k in d and d[k] not in (None,''):
            return d[k]
    return None

def expiry_key(v):
    if not v: return None
    s=str(v).strip()
    for fmt in ('%d-%b-%Y','%d-%b-%y','%d/%m/%Y','%Y-%m-%d','%Y-%m-%dT%H:%M:%S'):
        try: return datetime.strptime(s[:19],fmt).date()
        except: pass
    return None

def normalize_expiry(v):
    d=expiry_key(v)
    return d.isoformat() if d else str(v)[:10]

def parse_symbol_payload(symbol,payload):
    data=payload.get('data',[]) if isinstance(payload,dict) else []
    if not isinstance(data,list): data=[]
    futures=[]
    spot=num(pick(payload,'underlyingValue','underlying_value','underlyingPrice','spotPrice','spot'))
    if spot is None and isinstance(payload.get('records'),dict):
        spot=num(pick(payload['records'],'underlyingValue','underlying_value','underlyingPrice'))
    for d in data:
        if not isinstance(d,dict): continue
        inst=str(pick(d,'instrumentType','instrument','type') or '').upper()
        if 'FUTSTK' not in inst and inst not in ('FUT','FUTURES'):
            continue
        exp=pick(d,'expiryDate','expiry_date','expiry','expiryDateTime')
        ed=expiry_key(exp)
        px=num(pick(d,'lastPrice','ltp','LTP','last_trade_price','last'))
        if ed is None or px is None: continue
        if spot is None:
            spot=num(pick(d,'underlyingValue','underlying_value','underlyingPrice','spotPrice','spot'))
        futures.append((ed,px))
    # Some NSE payloads expose the underlying value only inside non-futures rows.
    if spot is None:
        for d in data:
            if isinstance(d,dict):
                spot=num(pick(d,'underlyingValue','underlying_value','underlyingPrice','spotPrice','spot'))
                if spot is not None: break
    byexp={ed:px for ed,px in futures}
    arr=sorted(byexp.items(),key=lambda x:x[0])
    if spot is None or len(arr)<2:
        return None
    near_exp,near=arr[0]; next_exp,nxt=arr[1]
    far_exp,far=arr[2] if len(arr)>2 else (None,None)
    return {'symbol':symbol,'spot':spot,'near':near,'next':nxt,'far':far,
            'spotNear':spot-near,'spotNext':spot-nxt,'nearNext':near-nxt,
            'nearExpiry':near_exp.isoformat(),'nextExpiry':next_exp.isoformat(),'farExpiry':far_exp.isoformat() if far_exp else ''}

client=NSEClient()
state={'rows':[],'updated':'','error':'Starting…','busy':False,'symbols':0,'ok':0,'fail':0}
state_lock=threading.Lock()


def refresh():
    with state_lock:
        if state['busy']: return
        state['busy']=True
        state['error']='Refreshing…'
    try:
        syms=client.symbols()
        if not syms: raise RuntimeError('NSE returned no F&O stock symbols.')
        rows=[]; failures=0
        def one(s):
            try:
                p=client.derivatives(s)
                return parse_symbol_payload(s,p)
            except Exception:
                return None
        # Moderate concurrency to reduce total refresh time while avoiding a request storm.
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            for result in ex.map(one,syms):
                if result is None: failures += 1
                else: rows.append(result)
        rows.sort(key=lambda x:x['symbol'])
        if not rows:
            raise RuntimeError(f'NSE symbol list loaded ({len(syms)}), but no futures quotes could be parsed. NSE may be blocking automated requests.')
        with state_lock:
            state['rows']=rows; state['updated']=datetime.now().strftime('%d-%b-%Y %H:%M:%S'); state['symbols']=len(syms); state['ok']=len(rows); state['fail']=failures; state['error']=''
    except Exception as e:
        with state_lock: state['error']=f'{type(e).__name__}: {e}'
    finally:
        with state_lock: state['busy']=False


def worker():
    while True:
        refresh(); time.sleep(REFRESH)

HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Live NSE Stock Futures Dashboard</title>
<style>body{font-family:Arial,Helvetica,sans-serif;margin:0;background:#f5f7fb;color:#162033}.wrap{max-width:1700px;margin:auto;padding:22px}.title{font-size:30px;font-weight:800}.sub{color:#64748b;margin:4px 0 18px}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.card{background:#fff;padding:16px;border-radius:12px;box-shadow:0 2px 10px #0000000d}.label{font-size:13px;color:#64748b}.value{font-size:25px;font-weight:800;margin-top:5px}.bar{margin:14px 0;background:#fff;padding:12px;border-radius:12px;display:flex;gap:10px;align-items:center;flex-wrap:wrap}.bar input,.bar select,.bar button{padding:9px 11px;border:1px solid #cbd5e1;border-radius:8px;background:#fff}.bar button{background:#2563eb;color:white;border:0;cursor:pointer}.err{color:#c62828;font-weight:600}.ok{color:#16803c;font-weight:700}.muted{color:#64748b}.tablewrap{background:#fff;border-radius:12px;overflow:auto;box-shadow:0 2px 10px #0000000d}table{width:100%;border-collapse:collapse;min-width:1100px}th{position:sticky;top:0;background:#e9eef6;z-index:2;cursor:pointer}th,td{padding:10px 9px;border-bottom:1px solid #e5e7eb;text-align:right;white-space:nowrap}th:nth-child(1),td:nth-child(1),th:nth-child(2),td:nth-child(2){text-align:left}.pos{color:#16803c;font-weight:700}.neg{color:#c62828;font-weight:700}.qual{background:#ecfdf3}.small{font-size:12px;color:#64748b;margin-top:8px}@media(max-width:900px){.cards{grid-template-columns:repeat(2,1fr)}.title{font-size:24px}}</style></head>
<body><div class="wrap"><div class="title">Live NSE Stock Futures Dashboard</div><div class="sub">All NSE stock-futures underlyings • automatic live refresh • Spot − Near • Spot − Next • Near − Next</div>
<div class="cards"><div class="card"><div class="label">Stock Futures</div><div class="value" id="count">0</div></div><div class="card"><div class="label">Qualifying</div><div class="value" id="qual">0</div></div><div class="card"><div class="label">Lowest Near − Next</div><div class="value" id="low">—</div></div><div class="card"><div class="label">Last Update</div><div class="value" id="updated">—</div></div></div>
<div class="bar"><input id="search" placeholder="Search symbol" oninput="render()"><select id="filter" onchange="render()"><option value="all">All stocks</option><option value="qual">Qualifying: Spot &gt; Near &amp; Next + Near−Next &gt; 0</option><option value="positive">Near−Next &gt; 0</option></select><button onclick="load()">Refresh now</button><span id="status" class="muted">Starting…</span></div>
<div class="tablewrap"><table><thead><tr><th>#</th><th>Symbol</th><th>Spot</th><th>Near</th><th>Next</th><th>Far</th><th>Spot−Near</th><th>Spot−Next</th><th>Near−Next</th></tr></thead><tbody id="body"></tbody></table></div><div class="small">NSE public endpoints are session-protected and may change/rate-limit. This dashboard refreshes the complete stock-futures set about every 30 seconds to reduce blocking. Prices should be verified before trading.</div></div>
<script>let data=[],sortKey='nearNext',asc=true;const n=x=>x==null?'—':Number(x).toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2});async function load(){try{let r=await fetch('/data?t='+Date.now());let j=await r.json();document.getElementById('count').textContent=j.symbols||data.length;document.getElementById('updated').textContent=j.updated||'—';if(j.error){document.getElementById('status').className='err';document.getElementById('status').textContent='Data status: '+j.error;if(j.rows&&j.rows.length)data=j.rows}else{data=j.rows||[];document.getElementById('status').className='ok';document.getElementById('status').textContent='Live • '+data.length+' futures loaded • '+(j.fail||0)+' unavailable • refresh ~30s'}render()}catch(e){document.getElementById('status').className='err';document.getElementById('status').textContent='Connection error: '+e}}function render(){let q=document.getElementById('search').value.toUpperCase(),f=document.getElementById('filter').value;let rows=data.filter(x=>x.symbol.includes(q));if(f==='qual')rows=rows.filter(x=>x.spot>x.near&&x.spot>x.next&&x.nearNext>0);if(f==='positive')rows=rows.filter(x=>x.nearNext>0);rows.sort((a,b)=>{let av=a[sortKey],bv=b[sortKey];if(typeof av==='string')return String(av).localeCompare(String(bv))*(asc?1:-1);return ((av??Infinity)-(bv??Infinity))*(asc?1:-1)});let qs=data.filter(x=>x.spot>x.near&&x.spot>x.next&&x.nearNext>0);document.getElementById('qual').textContent=qs.length;document.getElementById('low').textContent=qs.length?n(Math.min(...qs.map(x=>x.nearNext))):'—';document.getElementById('body').innerHTML=rows.map((x,i)=>{let q=x.spot>x.near&&x.spot>x.next&&x.nearNext>0;return `<tr class="${q?'qual':''}"><td>${i+1}</td><td><b>${x.symbol}</b></td><td>${n(x.spot)}</td><td>${n(x.near)}</td><td>${n(x.next)}</td><td>${n(x.far)}</td><td class="${x.spotNear>=0?'pos':'neg'}">${n(x.spotNear)}</td><td class="${x.spotNext>=0?'pos':'neg'}">${n(x.spotNext)}</td><td class="${x.nearNext>=0?'pos':'neg'}">${n(x.nearNext)}</td></tr>`}).join('')}document.querySelectorAll('th').forEach((th,i)=>th.onclick=()=>{const keys=['rank','symbol','spot','near','next','far','spotNear','spotNext','nearNext'];if(keys[i]&&keys[i]!=='rank'){if(sortKey===keys[i])asc=!asc;else{sortKey=keys[i];asc=true}render()}});load();setInterval(load,10000);</script></body></html>'''

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/data'):
            with state_lock: body=json.dumps({'rows':state['rows'],'updated':state['updated'],'error':state['error'],'symbols':state['symbols'],'ok':state['ok'],'fail':state['fail']},ensure_ascii=False).encode()
            self.send_response(200); self.send_header('Content-Type','application/json'); self.send_header('Cache-Control','no-store'); self.end_headers(); self.wfile.write(body); return
        body=HTML.encode(); self.send_response(200); self.send_header('Content-Type','text/html; charset=utf-8'); self.end_headers(); self.wfile.write(body)
    def log_message(self,*args): pass

if __name__=='__main__':
    print('LIVE NSE STOCK FUTURES DASHBOARD V6')
    print('NSE NextApi: getSymbolDerivativesData + underlying-information')
    print(f'Refresh interval: {REFRESH}s | workers: {MAX_WORKERS}')
    print('Starting first data refresh. This may take 20-60 seconds...')
    refresh()
    if state['error']: print('DATA ERROR:',state['error'])
    else: print('Loaded',len(state['rows']),'stock futures from',state['symbols'],'F&O underlyings')
    threading.Thread(target=worker,daemon=True).start()
    server=ThreadingHTTPServer((HOST,PORT),Handler)
    print(f'Listening on {HOST}:{PORT}')
    # Render provides the public HTTPS URL; do not attempt to open a browser on the server.
    server.serve_forever()
