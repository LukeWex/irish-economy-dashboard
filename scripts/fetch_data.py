#!/usr/bin/env python3
import json, os, sys, time, math
from datetime import datetime, timezone
import requests
from pyjstat import pyjstat

def pxstat_jsonstat(table):
  url = f"https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/{table}/JSON-stat/2.0/en"
  r = requests.get(url, timeout=60)
  r.raise_for_status()
  return r.json()

def px_to_df(table):
  js = pxstat_jsonstat(table)
  ds = pyjstat.Dataset.read(json.dumps(js))
  df = ds.write('dataframe')
  return df

def last_pair(df, time_col, value_col, dropna=True):
  sub = df[[time_col, value_col]].dropna() if dropna else df[[time_col, value_col]]
  return sub.iloc[-1][time_col], float(sub.iloc[-1][value_col])

def series_xy(df, time_col, value_col):
  sub = df[[time_col, value_col]].dropna()
  return sub[time_col].tolist(), [float(v) for v in sub[value_col].tolist()]

def try_dof():
  urls = [
    "https://databank.finance.gov.ie/OpenDataSourceCSV?report=TaxYrOnYr",
    "http://databank.finance.gov.ie/FinDataBank.aspx?rep=OpenDataSourceCSV"
  ]
  for u in urls:
    try:
      r = requests.get(u, timeout=60)
      if r.ok and "csv" in r.headers.get("content-type","").lower():
        return r.text
      if r.ok and ("," in r.text or "\t" in r.text):
        return r.text
    except Exception:
      continue
  return None

def parse_dof(csv_text):
  import csv, io
  reader = csv.DictReader(io.StringIO(csv_text))
  month_keys = ["Period", "Month"]
  year_keys = ["Year", "FiscalYear", "Year "]
  value_keys = ["TotalReceipts","Total","Total Receipts","Total_Receipts","Receipts","Amount"]
  rows = [r for r in reader]
  data = {}
  for r in rows:
    m = next((r[k] for k in month_keys if k in r and r[k]), None)
    y = next((r[k] for k in year_keys if k in r and r[k]), None)
    v_raw = next((r[k] for k in value_keys if k in r and r[k]), None)
    if not (m and y and v_raw): continue
    try: v = float(str(v_raw).replace(',',''))
    except: continue
    key = f"{y}-{m}"
    data[key] = v
  if not data: return None
  labels = sorted(data.keys(), key=lambda s: datetime.strptime(s+"-01","%Y-%b-%d") if len(s.split('-')[1])==3 else datetime.strptime(s+"-01","%Y-%m-%d"))
  vals = [data[k] for k in labels]
  return {
    "x": labels,
    "total": vals,
    "latest_total_month": labels[-1],
    "latest_total_value": vals[-1]
  }

def safe_px(table, filters=None, label=None):
  try:
    df = px_to_df(table)
    if df.empty:
      print(f"[WARN] '{label or table}' returned empty DataFrame.")
      return None
    if filters:
      for col_filter, val in filters.items():
        for col in df.columns:
          if col_filter.lower() in col.lower():
            df = df[df[col].str.contains(val, case=False, na=False)]
    return df
  except Exception as e:
    print(f"[ERROR] Could not load '{label or table}': {e}")
    return None

def main(out_path):
  snap = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "series": {}
  }

  # Unemployment
  df = safe_px("MUM01", {
    "sex": "All persons",
    "season": "Seasonally adjusted",
    "age": "15-74",
    "stat": "rate"
  }, "unemployment")
  if df is not None:
    try:
      time_col = next(c for c in df.columns if "time" in c.lower())
      x, y = series_xy(df, time_col, "value")
      date, val = last_pair(df, time_col, "value")
      snap["series"]["unemployment_rate"] = {
        "x": x, "y": y,
        "latest": val,
        "latest_date": date
      }
    except Exception as e:
      print(f"[ERROR] unemployment parsing: {e}")

  # GNI*
  df = safe_px("NA002", label="gni_star")
  if df is not None:
    try:
      for c in df.columns:
        if c.lower().startswith("indicator") or "stat" in c.lower():
          df = df[df[c].str.contains("Modified gross national income|gni", case=False, na=False)]
      tcol = next(c for c in df.columns if "time" in c.lower() or "year" in c.lower())
      x, y = series_xy(df, tcol, "value")
      snap["series"]["gni_star"] = {
        "x": x, "y": y,
        "latest_year": x[-1], "latest_value": y[-1]
      }
    except Exception as e:
      print(f"[ERROR] GNI* parsing: {e}")

  # Wage growth
  df = safe_px("EHQ03", label="wage_growth")
  if df is not None:
    try:
      for c in df.columns:
        if "earnings" in c.lower(): df = df[df[c].str.contains("Average weekly earnings", case=False, na=False)]
        if "sector" in c.lower(): df = df[df[c].str.contains("All", case=False, na=False)]
      tcol = next(c for c in df.columns if "time" in c.lower())
      ser = df["value"].astype(float).tolist()
      dates = df[tcol].tolist()
      yoy = [None]*4
      for i in range(4, len(ser)):
        if ser[i-4] not in (None, 0):
          yoy.append((ser[i]/ser[i-4])-1.0)
        else:
          yoy.append(None)
      snap["series"]["wage_growth"] = {
        "x": dates, "y": yoy,
        "latest_period": dates[-1],
        "latest_yoy": yoy[-1]
      }
    except Exception as e:
      print(f"[ERROR] wage growth parsing: {e}")

  # MDD
  df = safe_px("NQQ46", label="mdd")
  if df is not None:
    try:
      for c in df.columns:
        if "indicator" in c.lower(): df = df[df[c].str.contains("modified total domestic demand", case=False, na=False)]
      tcol = next(c for c in df.columns if "time" in c.lower())
      x, y = series_xy(df, tcol, "value")
      snap["series"]["mdd"] = { "x": x, "y": y }
    except Exception as e:
      print(f"[ERROR] MDD parsing: {e}")

  # Current account
  df = safe_px("BPQ15", label="current_account")
  if df is not None:
    try:
      for c in df.columns:
        if "balance" in c.lower(): df = df[df[c].str.contains("Balance on Current Account", case=False, na=False)]
      tcol = next(c for c in df.columns if "time" in c.lower())
      x, y = series_xy(df, tcol, "value")
      y = [v / 1000.0 for v in y]
      snap["series"]["current_account"] = { "x": x, "y": y }
    except Exception as e:
      print(f"[ERROR] Current account parsing: {e}")

  # Employment
  df = safe_px("ALF01", { "sex": "All persons" }, "employment_rate")
  if df is not None:
    try:
      tcol = next(c for c in df.columns if "time" in c.lower())
      x, y = series_xy(df, tcol, "value")
      snap["series"]["employment_rate"] = { "x": x, "y": y }
    except Exception as e:
      print(f"[ERROR] Employment parsing: {e}")

  # Live Register
  df = safe_px("LRM02", { "season": "Seasonally adjusted" }, "live_register")
  if df is not None:
    try:
      tcol = next(c for c in df.columns if "time" in c.lower())
      x, y = series_xy(df, tcol, "value")
      y = [v / 1000.0 for v in y]
      snap["series"]["live_register"] = { "x": x, "y": y }
    except Exception as e:
      print(f"[ERROR] Live Register parsing: {e}")

  # Housing completions + permissions
  dfN = safe_px("NDQ01", { "type": "Total" }, "housing_completions")
  dfP = safe_px("BHQ05", { "type": "Total dwellings" }, "housing_permissions")
  try:
    xN, yN = series_xy(dfN, next(c for c in dfN.columns if "time" in c.lower()), "value") if dfN is not None else ([], [])
    xP, yP = series_xy(dfP, next(c for c in dfP.columns if "time" in c.lower()), "value") if dfP is not None else ([], [])
    snap["series"]["housing"] = {
      "completions": {"x": xN, "y": yN},
      "permissions": {"x": xP, "y": yP}
    }
  except Exception as e:
    print(f"[ERROR] Housing parsing: {e}")

  # HICP Ireland
  df = safe_px("HPM01", label="hicp_ireland")
  xIE, yIE = [], []
  if df is not None:
    try:
      for c in df.columns:
        if "rate" in c.lower(): df = df[df[c].str.contains("Annual|All-items", case=False, na=False)]
      tcol = next(c for c in df.columns if "time" in c.lower())
      xIE, yIE = series_xy(df, tcol, "value")
    except Exception as e:
      print(f"[ERROR] HICP Ireland parsing: {e}")

  # HICP EA19
  try:
    ecb_url = "https://sdw.ecb.europa.eu/servlet/data/ICP/M.U2.N.000000.4.ANR?lastNObservations=72&format=jsondata"
    r = requests.get(ecb_url, timeout=60)
    r.raise_for_status()
    ej = r.json()
    time_vals = [v["id"] for v in ej["structure"]["dimensions"]["observation"][0]["values"]]
    obs = ej["dataSets"][0]["series"]
    s_key = list(obs.keys())[0]
    o = obs[s_key]["observations"]
    ea_y = [o[i][0] if i in o else None for i in range(len(time_vals))]
  except Exception as e:
    print(f"[ERROR] HICP EA19 parsing: {e}")
    time_vals, ea_y = [], []

  snap["series"]["hicp"] = {
    "ireland": {"x": xIE, "y": yIE},
    "ea19": {"x": time_vals, "y": ea_y}
  }

  # Tax receipts
  try:
    dof_csv = try_dof()
    if dof_csv:
      parsed = parse_dof(dof_csv)
      if parsed:
        snap["series"]["tax_receipts"] = parsed
  except Exception as e:
    print(f"[ERROR] DoF receipts parsing: {e}")

  # Save snapshot
  with open(out_path, "w", encoding="utf-8") as f:
    json.dump(snap, f, indent=2)

if __name__ == "__main__":
  out = sys.argv[1] if len(sys.argv) > 1 else "data/snapshot.json"
  main(out)
