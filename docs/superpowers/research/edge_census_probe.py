import sys
sys.path[:0]=['src','benchmarks']
import osn_sample
from osn_sample import build_spark, load_dotenv
from pyspark.sql import functions as F
import track_link_grid as g, track_link_controls as c
from track_truth import load_flight_intervals
from opdi.pipeline.segmentation import SegmentationParams
load_dotenv(); osn_sample.RESEARCH_EXECUTORS=8
s=build_spark(4,'8g',distributed=True); s.sparkContext.setLogLevel('ERROR'); s.conf.set('spark.sql.session.timeZone','UTC')
p=SegmentationParams()
out=[]
for day in ('2026-06-14','2025-06-14'):
    tracks=g.read_tracks(s,'s3a://eurocontrol/opdi-world/osn_tracks',day)
    truth=load_flight_intervals(s,g.truth_months(day),[day],reference_base='s3a://eurocontrol/opdi/research/reference').cache()
    holes=c.label_real_holes(c.real_hole_candidates(tracks,p),truth)
    cols=[x for x in ['icao24','gap_min','position_gap_min','gc_nm','alt_before_ft','alt_after_ft','alt_new_edge_ft','vr_before_fpm','vr_after_fpm','vr_before_samples','vr_after_samples','spd_before_kt','cs_before','cs_after','label','t_old_edge','t_new_edge'] if x in holes.columns]
    pdf=holes.select(*cols).toPandas(); pdf['day']=day; out.append(pdf); print(day,len(pdf),flush=True)
import pandas as pd
d=pd.concat(out); d.to_parquet(sys.argv[1]); print(d.label.value_counts().to_string()); s.stop()
