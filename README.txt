LIVE NSE STOCK FUTURES DASHBOARD V6

1. Extract this folder.
2. Double-click run_dashboard.bat.
3. Keep the black command window open.
4. Open http://127.0.0.1:8765 if the browser does not open automatically.

Features:
- Automatically gets the current NSE F&O underlying stock universe.
- Fetches stock-futures contracts through NSE's current NextApi derivative endpoint.
- Uses the first 3 available stock-futures expiries as Near / Next / Far.
- Calculates Spot-Near, Spot-Next and Near-Next.
- Refreshes approximately every 30 seconds.
- No Flask, Requests, pip, or third-party packages required.

NSE public endpoints are unofficially consumed by this local tool and can be changed or rate-limited by NSE. If NSE blocks automated requests, the dashboard will show the exact status rather than silently showing stale prices.
