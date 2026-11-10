"""Along-track / cross-track offset between ADS-C reports and interpolated ADS-B."""
import numpy as np, pandas as pd, pyarrow.fs as fs, pyarrow.parquet as pq, datetime as dt, sys

def env(p="/home/jupyter/work/opdi/.env"):
    d={}
    for line in open(p):
        line=line.strip()
        if line and not line.startswith("#") and "=" in line:
            k,_,v=line.partition("="); d[k.strip()]=v.strip()
    return d
e=env()
s3 = fs.S3FileSystem(access_key=e["AWS_ACCESS_KEY_ID"], secret_key=e["AWS_SECRET_ACCESS_KEY"],
                     endpoint_override="https://s3.opensky-network.org", scheme="https", region="us-east-1")

HOURS = [int(dt.datetime(2026,6,1,h,tzinfo=dt.timezone.utc).timestamp()) for h in (11,12,13)]
RG = sys.argv[1] if len(sys.argv)>1 else "0"

adsc = pd.read_parquet("adsc_2026-06-01.parquet")
adsc = adsc[(adsc.time >= HOURS[1]) & (adsc.time < HOURS[1]+3600)].copy()
adsc["hex"] = adsc.hex.str.lower()
print(f"ADS-C reports in 12:00-13:00 UTC: {len(adsc)} from {adsc.hex.nunique()} airframes")

want = set(adsc.hex)
cols = ["time","icao24","lat","lon","velocity","heading","lastPosUpdate"]
parts=[]
for h in HOURS:
    import re
    sys.path.insert(0,'.')
    from s3ls import request
    st, body = request("/opensky-hdfs-backup/", {"list-type":"2","max-keys":"10",
        "prefix":f"tables_v4/state_vectors/hour={h}/"})
    key = re.findall(r"<Key>(.*?)</Key>", body)[0]
    pf = pq.ParquetFile(s3.open_input_file("opensky-hdfs-backup/"+key))
    groups = range(pf.metadata.num_row_groups) if RG == "all" else [int(RG)]
    kept = 0
    for g in groups:
        t = pf.read_row_group(g, columns=cols).to_pandas()
        t = t[t.icao24.isin(want)]
        kept += len(t)
        parts.append(t)
    print(f"  hour={h} rg={RG}: kept {kept:,} ADS-B rows for our airframes", flush=True)
adsb = pd.concat(parts, ignore_index=True).dropna(subset=["lat","lon"])
adsb = adsb.sort_values(["icao24","time"])
print(f"ADS-B rows total {len(adsb):,} for {adsb.icao24.nunique()} airframes")

R = 6371000.0
def enu(lat, lon, lat0, lon0):
    """Local east/north metres relative to (lat0, lon0)."""
    x = np.radians(lon-lon0)*R*np.cos(np.radians(lat0))
    y = np.radians(lat-lat0)*R
    return x, y

recs=[]
for h, grp in adsb.groupby("icao24", sort=False):
    cr = adsc[adsc.hex==h]
    if cr.empty: continue
    t = grp.time.to_numpy(dtype=float); la = grp.lat.to_numpy(); lo = grp.lon.to_numpy()
    vel = grp.velocity.to_numpy(); hdg = grp.heading.to_numpy()
    for _, c in cr.iterrows():
        tc = float(c.time)
        i = np.searchsorted(t, tc)
        if i == 0 or i >= len(t): continue
        t0, t1 = t[i-1], t[i]
        if tc - t0 > 60 or t1 - tc > 60: continue      # need tight bracketing
        if t1 == t0: continue
        w = (tc - t0)/(t1 - t0)
        lat_b = la[i-1] + w*(la[i]-la[i-1])
        lon_b = lo[i-1] + w*(lo[i]-lo[i-1])
        v = np.nanmean([vel[i-1], vel[i]])             # m/s
        # track direction from the ADS-B pair itself (robust to a bad heading)
        dx, dy = enu(la[i], lo[i], la[i-1], lo[i-1])
        n = np.hypot(dx, dy)
        if n < 1.0 or not np.isfinite(v) or v < 50: continue
        ux, uy = dx/n, dy/n
        ox, oy = enu(c.lat, c.lon, lat_b, lon_b)       # ADS-C minus ADS-B
        along = ox*ux + oy*uy
        cross = -ox*uy + oy*ux
        recs.append(dict(icao24=h, t=tc, bracket_s=t1-t0,
                         along_m=along, cross_m=cross,
                         dist_m=np.hypot(ox,oy), v_ms=v,
                         implied_dt_s=along/v,
                         gs_adsc_kt=c.gs, lastPosUpdate=grp.lastPosUpdate.iloc[i],
                         alt_ft=c.alt_baro_ft))
m = pd.DataFrame(recs)
m.to_parquet(f"sync_pairs_{RG}.parquet")
print(f"\nmatched ADS-C reports with tight ADS-B brackets: {len(m)} "
      f"({m.icao24.nunique() if len(m) else 0} airframes)")
if len(m):
    print("\n--- offsets (metres) ---")
    print(m[["along_m","cross_m","dist_m"]].describe(percentiles=[.05,.25,.5,.75,.95]).round(1).to_string())
    print("\n--- implied time offset (s), = along-track / groundspeed ---")
    print(m.implied_dt_s.describe(percentiles=[.05,.25,.5,.75,.95]).round(2).to_string())
    print("\nmedian |cross| %.0f m vs median |along| %.0f m" %
          (m.cross_m.abs().median(), m.along_m.abs().median()))
