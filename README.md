# Irish Economy & Fiscal Dashboard (GitHub Pages)

A static, interactive dashboard for key Irish macro + fiscal indicators, automatically updated daily at **08:00 Dublin time** (07:00 UTC in summer).

**Live update path:** `data/snapshot.json`  
**Frontend:** Plotly + Bootstrap (no build tools).

## Deploy to your GitHub

1. Create a repo named e.g. `irish-economy-dashboard`.
2. Upload this folder's contents to the root of your repo (or push via git).
3. Enable Pages: Settings → Pages → Source: `Deploy from a branch`, Branch: `main` (or `master`), Folder: `/ (root)`.
4. Wait 1–2 minutes and open the URL shown (e.g. `https://<username>.github.io/irish-economy-dashboard/`).

## Automation (GitHub Actions)

- The workflow `.github/workflows/update-data.yml` runs daily at **07:00 UTC** (08:00 Dublin during summer) and on manual dispatch.
- It executes `scripts/fetch_data.py` which fetches:
  - CSO PxStat: MUM01 (Unemployment), NA002 (GNI*), EHQ03 (EHECS), NQQ46 (MDD),
    BPQ15 (Current Account), ALF01 (Employment rate), LRM02 (Live Register),
    NDQ01 (Completions), BHQ05 (Permissions), HPM01 (HICP IE).
  - ECB SDW: EA19 HICP y/y.
  - **Dept. of Finance**: best-effort CSV via DataBank endpoints. If DoF source is unavailable, the script leaves the last snapshot as-is.

### Notes on Department of Finance data
Dept. of Finance endpoints can change. If `tax_receipts` is missing:
- Manually drop the latest CSV/XLS values into the script parser, or
- Edit `scripts/fetch_data.py` to point at a stable CSV URL for your needs,
- Or push a manual `data/snapshot.json` update; the site will serve it immediately.

## Local test
Open `index.html` directly or via a local server. The frontend reads `/data/snapshot.json` from the same origin (no CORS issues).

---

Built for quick triage of macro conditions affecting Ireland’s economy and fiscal stance. Data © CSO, Department of Finance, ECB.
