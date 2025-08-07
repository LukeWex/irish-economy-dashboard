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
  ds = pyjstat.Dataset.read(json.dumps(js))  # ← Fix: convert dict to JSON string
  return ds.write('dataframe')

def last_pair(df, time_col, value_col, dropna=True):
  sub = df[[time_col, value_col]].dropna() if dropna else df[[time_col, value_col]]
  return sub.iloc[-1][time_col], float(sub.iloc[-1][value_col])

def series_xy(df, time_col, value_col):
  sub = df[[time_col, value_col]].dropna()
  return sub[time_col].tolist(), [float(v) for v in sub[value_col].tolist()]

def try_dof():
  # Best-effort: attempt a couple of known CSV endpoints.
  # If everything fails, return None so we keep prior values.
  urls = [
    "https://databank.finance.gov.ie/OpenDataSourceCSV?report=TaxYrOnYr",
    "http://databank.finance.gov.ie/FinDataBank.aspx?rep=OpenDataSourceCSV"
  ]
  for u in urls:
    try:
      r = requests.get(u, timeout=60)
      if r.ok and "csv" in r.headers.get("content-type","").lower():
        return r.text
      # Some endpoints return text/csv without header; accept if it looks CSV-ish
      if r.ok and ("," in r.text or "\t" in r.text):
        return r.text
    except Exception as e:
      continue
  return None

def parse_dof(csv_text):
  # Very loose CSV parser for totals by month; we try multiple column names.
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
    if not (m and y and v_raw):
      continue
    try:
      v = float(str(v_raw).replace(',',''))
    except:
      continue
    key = f"{y}-{m}"
    data[key] = v
  if not data:
    return None
  labels = sorted(data.keys(), key=lambda s: datetime.strptime(s+"-01","%Y-%b-%d") if len(s.split('-')[1])==3 else datetime.strptime(s+"-01","%Y-%m-%d"))
  vals = [data[k] for k in labels]
  latest_month = labels[-1]
  latest_val = vals[-1]
  return {"x": labels, "total": vals, "latest_total_month": latest_month, "latest_total_value": latest_val}

def main(out_path):
  snap = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "series": {}
  }

  # Unemployment rate (MUM01): All persons, SA, 15-74, Rate
  df = px_to_df("MUM01")
  # Heuristic filters
  for col in df.columns:
    if col.lower().startswith("sex"): df = df[df[col].str.contains("All persons", case=False, na=False)]
    if "Season" in col or "seasonal" in col.lower(): df = df[df[col].str.contains("Seasonally adjusted", case=False, na=False)]
    if "Age" in col: df = df[df[col].str.contains("15-74", case=False, na=False)]
    if "stat" in col.lower(): df = df[df[col].str.contains("rate", case=False, na=False)]
  time_col = next(c for c in df.columns if "time" in c.lower())
  val_col = "value"
  x, y = series_xy(df, time_col, val_col)
  latest_date, latest = last_pair(df, time_col, val_col)
  snap["series"]["unemployment_rate"] = {"x": x, "y": y, "latest": latest, "latest_date": latest_date}

  # GNI* (NA002): Modified GNI, constant prices (million euro)
  df = px_to_df("NA002")
  # pick Modified GNI
  mask = None
  for c in df.columns:
    if c.lower().startswith("indicator") or "stat" in c.lower():
      mask = df[c].str.contains("Modified gross national income", case=False, na=False)
      if not mask.any():
        mask = df[c].str.contains("gni", case=False, na=False)
      df = df[mask]
  year_col = next(c for c in df.columns if "time" in c.lower() or "year" in c.lower())
  x, y = series_xy(df, year_col, "value")
  latest_year, latest_val = x[-1], y[-1]
  snap["series"]["gni_star"] = {"x": x, "y": y, "latest_year": latest_year, "latest_value": latest_val}

  # Wages – EHQ03 avg weekly earnings; take all employees/total and compute y/y
  df = px_to_df("EHQ03")
  for c in df.columns:
    if c.lower().startswith("type") or "stat" in c.lower():
      # keep Average weekly earnings
      df = df[df[c].str.contains("Average weekly earnings", case=False, na=False)]
    if "sector" in c.lower() or "section" in c.lower():
      df = df[df[c].str.contains("All", case=False, na=False)]
  tcol = next(c for c in df.columns if "time" in c.lower())
  ser = df[["value"]].reset_index(drop=True)["value"].astype(float).tolist()
  dates = df[tcol].tolist()
  yoy = [None]*4
  for i in range(4, len(ser)):
    if ser[i-4] not in (None, 0):
      yoy.append((ser[i]/ser[i-4])-1.0)
    else:
      yoy.append(None)
  snap["series"]["wage_growth"] = {"x": dates, "y": yoy, "latest_period": dates[-1], "latest_yoy": yoy[-1]}

  # MDD – NQQ46
  df = px_to_df("NQQ46")
  for c in df.columns:
    if c.lower().startswith("indicator") or "stat" in c.lower():
      df = df[df[c].str.contains("modified total domestic demand", case=False, na=False)]
  tcol = next(c for c in df.columns if "time" in c.lower())
  x,y = series_xy(df, tcol, "value")
  snap["series"]["mdd"] = {"x": x, "y": y}

  # Current Account – BPQ15 (€m) -> €bn
  df = px_to_df("BPQ15")
  for c in df.columns:
    if c.lower().startswith("indicator") or "stat" in c.lower():
      df = df[df[c].str.contains("Balance on Current Account", case=False, na=False)]
  tcol = next(c for c in df.columns if "time" in c.lower())
  x, y = series_xy(df, tcol, "value")
  y = [v/1000.0 for v in y]
  snap["series"]["current_account"] = {"x": x, "y": y}

  # Employment rate – ALF01 (%)
  df = px_to_df("ALF01")
  # choose Total/All persons
  for c in df.columns:
    if "sex" in c.lower() or "category" in c.lower():
      df = df[df[c].str.contains("All persons|Total", case=False, na=False)]
  tcol = next(c for c in df.columns if "time" in c.lower())
  x,y = series_xy(df, tcol, "value")
  snap["series"]["employment_rate"] = {"x": x, "y": y}

  # Live Register – LRM02 (SA)
  df = px_to_df("LRM02")
  for c in df.columns:
    if "season" in c.lower():
      df = df[df[c].str.contains("Seasonally adjusted", case=False, na=False)]
  tcol = next(c for c in df.columns if "time" in c.lower())
  x,y = series_xy(df, tcol, "value")
  y = [v/1000.0 for v in y]
  snap["series"]["live_register"] = {"x": x, "y": y}

  # Housing – NDQ01 (completions total), BHQ05 (permissions total)
  df = px_to_df("NDQ01")
  for c in df.columns:
    if "type" in c.lower() or "category" in c.lower():
      df = df[df[c].str.contains("Total", case=False, na=False)]
  tcol = next(c for c in df.columns if "time" in c.lower())
  xN, yN = series_xy(df, tcol, "value")

  df = px_to_df("BHQ05")
  for c in df.columns:
    if "type" in c.lower() or "category" in c.lower():
      df = df[df[c].str.contains("Total dwellings", case=False, na=False)]
  tcol = next(c for c in df.columns if "time" in c.lower())
  xP, yP = series_xy(df, tcol, "value")

  snap["series"]["housing"] = {"completions": {"x": xN, "y": yN}, "permissions": {"x": xP, "y": yP}}

  # HICP IE (HPM01 y/y) + EA19 from ECB SDW
  df = px_to_df("HPM01")
  # Keep All-items annual rate
  for c in df.columns:
    if "stat" in c.lower() or "indicator" in c.lower() or "rate" in c.lower():
      df = df[df[c].str.contains("Annual rate|y/y|YoY|12-month", case=False, na=False) | df[c].str.contains("All-items", case=False, na=False)]
  tcol = next(c for c in df.columns if "time" in c.lower())
  xIE, yIE = series_xy(df, tcol, "value")

  # EA19
  ecb_url = "https://sdw.ecb.europa.eu/servlet/data/ICP/M.U2.N.000000.4.ANR?lastNObservations=72&format=jsondata"
  r = requests.get(ecb_url, timeout=60)
  r.raise_for_status()
  ej = r.json()
  # Extract observations
  time_vals = [v["id"] for v in ej["structure"]["dimensions"]["observation"][0]["values"]]
  obs = ej["dataSets"][0]["series"]
  s_key = list(obs.keys())[0]
  o = obs[s_key]["observations"]
  ea_y = [o[i][0] if i in o else None for i in range(len(time_vals))]

  snap["series"]["hicp"] = {"ireland": {"x": xIE, "y": yIE}, "ea19": {"x": time_vals, "y": ea_y}}

  # DoF tax receipts (best effort)
  dof_csv = try_dof()
  if dof_csv:
    parsed = parse_dof(dof_csv)
    if parsed:
      snap["series"]["tax_receipts"] = {
        "x": parsed["x"],
        "total": parsed["total"],
        "latest_total_month": parsed["latest_total_month"],
        "latest_total_value": parsed["latest_total_value"]
      }

  with open(out_path, "w", encoding="utf-8") as f:
    json.dump(snap, f, indent=2)

if __name__ == "__main__":
  out = sys.argv[1] if len(sys.argv) > 1 else "data/snapshot.json"
  main(out)
