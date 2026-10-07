# NSE Stock Futures Dashboard — Cloud Deployment

This version is prepared to run as a public web service so the same dashboard can be opened from Windows, Android, tablet, etc.

## Render deployment
1. Create a GitHub repository and upload all files in this folder.
2. In Render, choose **New → Web Service** and connect that GitHub repository.
3. Use:
   - Runtime: Python
   - Build Command: leave blank
   - Start Command: `python server.py`
   - Plan: Free for testing
4. Deploy. Render gives the service a public `https://...onrender.com` URL.

The app binds to `0.0.0.0` and the `PORT` environment variable as required by Render.

## Important
The dashboard obtains data from NSE public endpoints. NSE can change endpoints, rate-limit, or block automated traffic. The service therefore shows a data error rather than fabricating prices if NSE is unavailable.

Render Free services can spin down after 15 minutes without inbound traffic, so the first mobile visit after inactivity can take about a minute to wake up.
