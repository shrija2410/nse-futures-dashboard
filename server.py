import csv
import io
import json
import os
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "10000"))

# Backend refresh interval
REFRESH = 15

# Reload Dhan instrument master once per hour
INSTR_REFRESH = 3600

CID = os.environ.get("DHAN_CLIENT_ID", "").strip()
TOKEN = os.environ.get("DHAN_ACCESS_TOKEN", "").strip()

MASTER = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
PROFILE = "https://api.dhan.co/v2/profile"
LTP = "https://api.dhan.co/v2/marketfeed/ltp"

IST = ZoneInfo("Asia/Kolkata")

state = {
    "rows": [],
    "updated": "",
    "error": "Starting…",
    "busy": False,
    "symbols": 0,
    "ok": 0,
    "fail": 0,
    "master_total": 0,
    "nse_futstk": 0,
    "spot_count": 0,
    "profile": {},
}

lock = threading.Lock()

instrument_rows = []
instrument_loaded_at = 0
instrument_lock = threading.Lock()


def now():
    return datetime.now(IST)


def num(value):
    try:
        if value in (None, ""):
            return None
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def parse_expiry(value):
    if value in (None, ""):
        return None

    s = str(value).strip()

    formats = (
        ("%Y-%m-%d", 10),
        ("%Y-%m-%d %H:%M:%S", 19),
        ("%d-%b-%Y", 11),
        ("%d/%m/%Y", 10),
        ("%d-%m-%Y", 10),
    )

    for fmt, length in formats:
        try:
            return datetime.strptime(s[:length], fmt).date()
        except Exception:
            pass

    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        return None


def http_get(url, timeout=60):
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/csv,text/plain,*/*",
        },
        method="GET",
    )

    with urlopen(request, timeout=timeout) as response:
        return response.read()


def read_http_error_body(error):
    try:
        return error.read().decode("utf-8", "replace").strip()[:1500]
    except Exception:
        return ""


def profile_check():
    """
    Dhan profile endpoint is used only for diagnostics.
    The access token is never printed.
    """

    if not TOKEN:
        raise RuntimeError("DHAN_ACCESS_TOKEN is missing")

    request = Request(
        PROFILE,
        headers={
            "Accept": "application/json",
            "access-token": TOKEN,
        },
        method="GET",
    )

    try:
        with urlopen(request, timeout=20) as response:
            raw = response.read()
            status_code = getattr(response, "status", 200)

    except HTTPError as error:
        body = read_http_error_body(error)
        message = f"Dhan Profile HTTP {error.code}"
        if body:
            message += f" | {body}"
        raise RuntimeError(message)

    except Exception as error:
        raise RuntimeError(f"Dhan Profile request failed: {error}")

    try:
        response_json = json.loads(raw.decode("utf-8", "replace"))
    except Exception:
        raise RuntimeError(
            f"Dhan Profile returned invalid JSON (HTTP {status_code})"
        )

    data = response_json.get("data", {})
    if not isinstance(data, dict):
        data = {}

    diagnostic = {
        "status": response_json.get("status"),
        "tokenValidity": data.get("tokenValidity"),
        "dataPlan": data.get("dataPlan"),
        "dataValidity": data.get("dataValidity"),
        "activeSegment": data.get("activeSegment"),
    }

    return diagnostic


def run_profile_check():
    try:
        diagnostic = profile_check()

        with lock:
            state["profile"] = diagnostic

        print(
            "DHAN PROFILE CHECK:",
            json.dumps(diagnostic, ensure_ascii=False),
            flush=True,
        )

        return diagnostic

    except Exception as error:
        message = str(error)

        with lock:
            state["profile"] = {"error": message}

        print("DHAN PROFILE ERROR:", message, flush=True)
        return None


def dhan_ltp(payload):
    if not CID:
        raise RuntimeError("DHAN_CLIENT_ID is missing")

    if not TOKEN:
        raise RuntimeError("DHAN_ACCESS_TOKEN is missing")

    request = Request(
        LTP,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": TOKEN,
            "client-id": CID,
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=20) as response:
            raw = response.read()
            status_code = getattr(response, "status", 200)

    except HTTPError as error:
        body = read_http_error_body(error)

        message = f"Dhan LTP HTTP {error.code}"

        if body:
            message += f" | {body}"

        raise RuntimeError(message)

    except Exception as error:
        raise RuntimeError(f"Dhan LTP request failed: {error}")

    if status_code != 200:
        raise RuntimeError(f"Dhan LTP HTTP {status_code}")

    try:
        response_json = json.loads(raw.decode("utf-8", "replace"))
    except Exception:
        raise RuntimeError("Dhan LTP returned invalid JSON")

    api_status = str(response_json.get("status", "")).lower()

    if api_status not in ("success", "true", ""):
        message = (
            response_json.get("message")
            or response_json.get("remarks")
            or response_json
        )
        raise RuntimeError(f"Dhan LTP API error: {message}")

    return response_json.get("data", {})


def load_instruments(force=False):
    global instrument_rows
    global instrument_loaded_at

    with instrument_lock:

        if (
            instrument_rows
            and not force
            and time.time() - instrument_loaded_at < INSTR_REFRESH
        ):
            return instrument_rows

        raw = http_get(MASTER)

        csv_text = raw.decode("utf-8-sig", "replace")
        reader = csv.DictReader(io.StringIO(csv_text))

        if not reader.fieldnames:
            raise RuntimeError("Dhan instrument master has no CSV headers")

        required_columns = {
            "EXCH_ID",
            "SEGMENT",
            "SECURITY_ID",
            "INSTRUMENT",
            "SYMBOL_NAME",
            "SM_EXPIRY_DATE",
        }

        missing = required_columns - set(reader.fieldnames)

        if missing:
            raise RuntimeError(
                "Dhan detailed master missing columns: "
                + ",".join(sorted(missing))
            )

        rows = []

        total = 0
        nse_futstk = 0
        spot_count = 0
        today = now().date()

        for row in reader:

            total += 1

            exchange = str(row.get("EXCH_ID", "")).strip().upper()
            segment = str(row.get("SEGMENT", "")).strip().upper()
            instrument = str(row.get("INSTRUMENT", "")).strip().upper()

            if exchange != "NSE":
                continue

            security_id = str(row.get("SECURITY_ID", "")).strip()

            if not security_id:
                continue

            # NSE cash/equity
            if instrument == "EQUITY" and segment == "E":

                symbol = str(row.get("SYMBOL_NAME", "")).strip().upper()

                if symbol:
                    rows.append(
                        ("spot", symbol, security_id, None, "")
                    )
                    spot_count += 1

            # NSE stock futures
            elif instrument == "FUTSTK" and segment == "D":

                nse_futstk += 1

                symbol = (
                    str(row.get("UNDERLYING_SYMBOL", ""))
                    .strip()
                    .upper()
                )

                if not symbol:
                    symbol = (
                        str(row.get("SYMBOL_NAME", ""))
                        .strip()
                        .upper()
                    )

                expiry = parse_expiry(row.get("SM_EXPIRY_DATE"))

                expiry_code = str(
                    row.get(
                        "SEM_EXPIRY_CODE",
                        row.get("EXPIRY_CODE", ""),
                    )
                ).strip()

                if symbol and expiry and expiry >= today:
                    rows.append(
                        (
                            "future",
                            symbol,
                            security_id,
                            expiry,
                            expiry_code,
                        )
                    )

        with lock:
            state["master_total"] = total
            state["nse_futstk"] = nse_futstk
            state["spot_count"] = spot_count

        if not any(item[0] == "future" for item in rows):
            raise RuntimeError(
                f"No NSE FUTSTK contracts parsed. "
                f"Master rows={total}, "
                f"NSE FUTSTK={nse_futstk}, "
                f"NSE equity={spot_count}."
            )

        instrument_rows = rows
        instrument_loaded_at = time.time()

        print(
            f"Dhan master: total={total}, "
            f"NSE FUTSTK={nse_futstk}, "
            f"NSE equity={spot_count}, "
            f"usable rows={len(rows)}",
            flush=True,
        )

        return rows


def build_contract_map():
    spots = {}
    futures = {}

    for instrument_type, symbol, security_id, expiry, expiry_code in load_instruments():

        if instrument_type == "spot":
            spots.setdefault(symbol, security_id)

        else:
            futures.setdefault(symbol, []).append(
                (expiry, security_id, expiry_code)
            )

    contracts = {}

    for symbol, entries in futures.items():

        entries = sorted(entries, key=lambda item: item[0])

        unique = []
        seen_expiries = set()

        for entry in entries:

            expiry = entry[0]

            if expiry in seen_expiries:
                continue

            seen_expiries.add(expiry)
            unique.append(entry)

        if len(unique) >= 2:

            contracts[symbol] = {
                "spot": spots.get(symbol),
                "near": unique[0],
                "next": unique[1],
                "far": unique[2] if len(unique) >= 3 else None,
            }

    print(
        f"Contract map: {len(contracts)} stocks "
        f"with >=2 NSE stock-future expiries",
        flush=True,
    )

    return contracts


def chunks(items, size=1000):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def get_quotes(contracts):

    instruments = []

    for contract in contracts.values():

        if contract["spot"]:
            instruments.append(
                ("NSE_EQ", contract["spot"])
            )

        for name in ("near", "next", "far"):

            if contract[name]:
                instruments.append(
                    ("NSE_FNO", contract[name][1])
                )

    quotes = {}

    for batch in chunks(instruments, 1000):

        payload = {}

        for segment, security_id in batch:
            payload.setdefault(segment, []).append(
                int(security_id)
            )

        data = dhan_ltp(payload)

        for segment, values in data.items():

            if not isinstance(values, dict):
                continue

            for security_id, value in values.items():

                if isinstance(value, dict):
                    price = num(value.get("last_price"))

                    if price is not None:
                        quotes[
                            (segment, str(security_id))
                        ] = price

        # Dhan Market Quote documented rate limit is 1 request/sec.
        time.sleep(1.05)

    return quotes


def make_rows(contracts, quotes):

    rows = []

    for symbol, contract in contracts.items():

        spot = None
        near = None
        nxt = None
        far = None

        if contract["spot"]:
            spot = quotes.get(
                ("NSE_EQ", str(contract["spot"]))
            )

        near = quotes.get(
            ("NSE_FNO", str(contract["near"][1]))
        )

        nxt = quotes.get(
            ("NSE_FNO", str(contract["next"][1]))
        )

        if contract["far"]:
            far = quotes.get(
                ("NSE_FNO", str(contract["far"][1]))
            )

        if spot is None or near is None or nxt is None:
            continue

        rows.append(
            {
                "symbol": symbol,
                "spot": spot,
                "near": near,
                "next": nxt,
                "far": far,
                "spotNear": spot - near,
                "spotNext": spot - nxt,
                "nearNext": near - nxt,
                "nearExpiry": contract["near"][0].isoformat(),
                "nextExpiry": contract["next"][0].isoformat(),
                "farExpiry": (
                    contract["far"][0].isoformat()
                    if contract["far"]
                    else ""
                ),
            }
        )

    return sorted(
        rows,
        key=lambda row: row["nearNext"]
    )


def refresh():

    with lock:

        if state["busy"]:
            return

        state["busy"] = True
        state["error"] = "Refreshing Dhan market data…"

    try:

        contracts = build_contract_map()

        if not contracts:
            raise RuntimeError(
                "No NSE stock-futures contracts with "
                "at least Near and Next expiry."
            )

        print(
            f"Requesting Dhan LTP for {len(contracts)} stocks...",
            flush=True,
        )

        quotes = get_quotes(contracts)

        rows = make_rows(contracts, quotes)

        if not rows:
            raise RuntimeError(
                "Dhan returned no usable stock-futures quotes. "
                "Check Dhan Data API access/entitlement and market hours."
            )

        qualifying = sum(
            1
            for row in rows
            if (
                row["spot"] > row["near"]
                and row["spot"] > row["next"]
                and row["nearNext"] > 0
            )
        )

        with lock:
            state.update(
                rows=rows,
                updated=now().strftime("%d-%b-%Y %H:%M:%S"),
                symbols=len(contracts),
                ok=len(rows),
                fail=max(0, len(contracts) - len(rows)),
                error="",
            )

        print(
            f"Dhan refresh OK: rows={len(rows)}, "
            f"qualifying={qualifying}",
            flush=True,
        )

    except Exception as error:

        message = f"{type(error).__name__}: {error}"

        with lock:
            state["error"] = message

        print("DATA ERROR:", message, flush=True)

    finally:

        with lock:
            state["busy"] = False


def worker():

    while True:

        time.sleep(REFRESH)
        refresh()


HTML = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Live NSE Stock Futures Dashboard</title>
<style>
body{font-family:Arial;margin:0;background:#f5f7fb;color:#162033}
.wrap{max-width:1700px;margin:auto;padding:22px}
.title{font-size:30px;font-weight:800}
.sub{color:#64748b;margin:4px 0 18px}
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}
.card{background:#fff;padding:16px;border-radius:12px}
.label{font-size:13px;color:#64748b}
.value{font-size:25px;font-weight:800;margin-top:5px}
.bar{margin:14px 0;background:#fff;padding:12px;border-radius:12px;
display:flex;gap:10px;align-items:center;flex-wrap:wrap}
.bar input,.bar select,.bar button{padding:9px 11px;border:1px solid #cbd5e1;
border-radius:8px;background:#fff}
.bar button{background:#2563eb;color:white;border:0}
.err{color:#c62828;font-weight:600}
.ok{color:#16803c;font-weight:700}
.tablewrap{background:#fff;border-radius:12px;overflow:auto}
table{width:100%;border-collapse:collapse;min-width:1100px}
th{position:sticky;top:0;background:#e9eef6;cursor:pointer}
th,td{padding:10px 9px;border-bottom:1px solid #e5e7eb;
text-align:right;white-space:nowrap}
th:nth-child(1),td:nth-child(1),
th:nth-child(2),td:nth-child(2){text-align:left}
.pos{color:#16803c;font-weight:700}
.neg{color:#c62828;font-weight:700}
.qual{background:#ecfdf3}
.small{font-size:12px;color:#64748b;margin-top:8px}
@media(max-width:900px){
.cards{grid-template-columns:repeat(2,1fr)}
.title{font-size:24px}
}
</style>
</head>
<body>
<div class="wrap">

<div class="title">Live NSE Stock Futures Dashboard</div>

<div class="sub">
Dhan live market data • All NSE stock-futures •
Spot − Near • Spot − Next • Near − Next
</div>

<div class="cards">

<div class="card">
<div class="label">Stock Futures</div>
<div class="value" id="count">0</div>
</div>

<div class="card">
<div class="label">Qualifying</div>
<div class="value" id="qual">0</div>
</div>

<div class="card">
<div class="label">Lowest Near − Next</div>
<div class="value" id="low">—</div>
</div>

<div class="card">
<div class="label">Last Update</div>
<div class="value" id="updated">—</div>
</div>

</div>

<div class="bar">

<input id="search"
placeholder="Search symbol"
oninput="render()">

<select id="filter" onchange="render()">

<option value="all">All stocks</option>

<option value="qual">
Qualifying: Spot &gt; Near &amp; Next + Near−Next &gt; 0
</option>

<option value="positive">
Near−Next &gt; 0
</option>

</select>

<button onclick="load()">Refresh now</button>

<span id="status">Starting…</span>

</div>

<div class="tablewrap">

<table>

<thead>

<tr>
<th>#</th>
<th>Symbol</th>
<th>Spot</th>
<th>Near</th>
<th>Next</th>
<th>Far</th>
<th>Spot−Near</th>
<th>Spot−Next</th>
<th>Near−Next</th>
</tr>

</thead>

<tbody id="body"></tbody>

</table>

</div>

<div class="small">
Data source: DhanHQ Market Quote API.
Prices should be verified before trading.
</div>

</div>

<script>

let data=[];
let sortKey="nearNext";
let asc=true;

const n=x =>
x==null
? "—"
: Number(x).toLocaleString("en-IN",
{
minimumFractionDigits:2,
maximumFractionDigits:2
});

async function load(){

try{

let response=await fetch("/data?t="+Date.now());
let json=await response.json();

document.getElementById("count").textContent =
json.symbols || data.length;

document.getElementById("updated").textContent =
json.updated || "—";

if(json.error){

document.getElementById("status").className="err";

document.getElementById("status").textContent =
"Data status: "+json.error;

if(json.rows && json.rows.length)
data=json.rows;

}else{

data=json.rows || [];

document.getElementById("status").className="ok";

document.getElementById("status").textContent =
"Live • "+data.length+
" futures loaded • "+
(json.fail||0)+
" unavailable • refresh ~15s";

}

render();

}catch(error){

document.getElementById("status").className="err";

document.getElementById("status").textContent =
"Connection error: "+error;

}

}

function render(){

let search =
document.getElementById("search").value.toUpperCase();

let filter =
document.getElementById("filter").value;

let rows =
data.filter(x=>x.symbol.includes(search));

if(filter==="qual")
rows=rows.filter(x=>
x.spot>x.near &&
x.spot>x.next &&
x.nearNext>0
);

if(filter==="positive")
rows=rows.filter(x=>x.nearNext>0);

rows.sort((a,b)=>
((a[sortKey]??Infinity)-
(b[sortKey]??Infinity))*
(asc?1:-1)
);

let qualifying =
data.filter(x=>
x.spot>x.near &&
x.spot>x.next &&
x.nearNext>0
);

document.getElementById("qual").textContent =
qualifying.length;

document.getElementById("low").textContent =
qualifying.length
? n(Math.min(...qualifying.map(x=>x.nearNext)))
: "—";

document.getElementById("body").innerHTML =
rows.map((x,i)=>{

let qualified =
x.spot>x.near &&
x.spot>x.next &&
x.nearNext>0;

return `<tr class="${qualified?"qual":""}">

<td>${i+1}</td>

<td><b>${x.symbol}</b></td>

<td>${n(x.spot)}</td>
<td>${n(x.near)}</td>
<td>${n(x.next)}</td>
<td>${n(x.far)}</td>

<td class="${x.spotNear>=0?"pos":"neg"}">
${n(x.spotNear)}
</td>

<td class="${x.spotNext>=0?"pos":"neg"}">
${n(x.spotNext)}
</td>

<td class="${x.nearNext>=0?"pos":"neg"}">
${n(x.nearNext)}
</td>

</tr>`;

}).join("");

}

document.querySelectorAll("th").forEach((th,index)=>{

th.onclick=()=>{

let key=[
"rank",
"symbol",
"spot",
"near",
"next",
"far",
"spotNear",
"spotNext",
"nearNext"
][index];

if(key && key!=="rank"){

if(sortKey===key)
asc=!asc;
else{
sortKey=key;
asc=true;
}

render();

}

};

});

load();
setInterval(load,10000);

</script>

</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):

    def do_GET(self):

        if self.path.startswith("/healthz"):

            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return

        if self.path.startswith("/data"):

            with lock:
                body = json.dumps(
                    state,
                    ensure_ascii=False
                ).encode("utf-8")

            self.send_response(200)
            self.send_header(
                "Content-Type",
                "application/json"
            )
            self.send_header(
                "Cache-Control",
                "no-store"
            )
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(200)
        self.send_header(
            "Content-Type",
            "text/html; charset=utf-8"
        )
        self.end_headers()
        self.wfile.write(HTML.encode("utf-8"))

    def log_message(self, *args):
        pass


if __name__ == "__main__":

    print(
        "LIVE NSE STOCK FUTURES DASHBOARD - DHAN FINAL",
        flush=True
    )

    print(
        f"Refresh: {REFRESH}s",
        flush=True
    )

    print(
        "DHAN_CLIENT_ID:",
        "FOUND" if CID else "MISSING",
        flush=True
    )

    print(
        "DHAN_ACCESS_TOKEN:",
        "FOUND" if TOKEN else "MISSING",
        flush=True
    )

    # IMPORTANT:
    # If profile authentication fails, do not repeatedly hit LTP.
    # This prevents the diagnostic loop from generating 429 errors.
    print(
        "Checking Dhan profile/token...",
        flush=True
    )

    profile = run_profile_check()

    if profile is None:

        with lock:
            state["error"] = (
                "Dhan authentication/profile check failed. "
                "See Render logs for the exact Dhan response."
            )

        print(
            "LTP requests will NOT start until the Dhan profile check succeeds.",
            flush=True
        )

    else:

        print(
            "Loading Dhan detailed instrument master...",
            flush=True
        )

        try:
            load_instruments()

            # Only attempt market data when profile authentication succeeded.
            refresh()

        except Exception as error:

            print(
                "STARTUP ERROR:",
                str(error),
                flush=True
            )

    threading.Thread(
        target=worker,
        daemon=True
    ).start()

    server = ThreadingHTTPServer(
        (HOST, PORT),
        Handler
    )

    print(
        "Listening on",
        PORT,
        flush=True
    )

    server.serve_forever()
