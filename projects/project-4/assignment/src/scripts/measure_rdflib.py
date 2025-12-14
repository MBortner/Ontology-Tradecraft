import pandas as pd
import sys
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD
from dateutil import parser as dateparser
import re
from pathlib import Path

# ---- Output Path ----
output_file = Path("projects/project-4/assignment/src/measure_cco.ttl")

# ---- Namespaces ----
EX = Namespace("http://example.org/")
CCO = Namespace("http://www.ontologyrepository.com/CommonCoreOntologies/")
BFO = Namespace("http://purl.obolibrary.org/obo/bfo.owl#")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")

# ---- Load your measurement data from CSV ----
csv_file = Path("projects/project-4/assignment/src/data/readings_normalized.csv")
print(f"[measure_rdflib] Loading data from: {csv_file}")

if not csv_file.exists():
    print(f"ERROR: {csv_file} not found. Run normalize_readings.py first!")
    sys.exit(1)

df = pd.read_csv(csv_file)
print(f"[measure_rdflib] Loaded {len(df)} measurements")

# ---- RDF Graph ----
g = Graph()
g.bind("ex", EX)
g.bind("cco", CCO)
g.bind("bfo", BFO)
g.bind("skos", SKOS)

# ---- Helper for safe URI creation ----
def clean(x):
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(x))

# ---- Track artifacts and qualities to avoid duplicates ----
artifacts_seen = set()
qualities_seen = set()

# ---- Build RDF triples ----
for idx, row in df.iterrows():

    artifact_id = clean(row["artifact_id"])
    quality_kind = clean(row["sdc_kind"])
    unit = clean(row["unit_label"])
    value = float(row["value"])
    tstamp = dateparser.parse(row["timestamp"])

    # URIs
    artifact_uri = EX[f"Artifact_{artifact_id}"]
    quality_uri = EX[f"{artifact_id}_{quality_kind}_Quality"]
    measurement_uri = EX[f"{artifact_id}_{quality_kind}_Measurement_{idx}"]
    unit_uri = EX[f"Unit_{unit}"]

    # Artifact (only add once)
    if artifact_uri not in artifacts_seen:
        g.add((artifact_uri, RDF.type, CCO.Artifact))
        g.add((artifact_uri, RDFS.label, Literal(row["artifact_id"])))
        artifacts_seen.add(artifact_uri)

    # Quality (only add once per artifact-quality pair)
    if quality_uri not in qualities_seen:
        # CRITICAL FIX: Use bfo:bearer_of instead of cco:has_quality
        g.add((artifact_uri, BFO.bearer_of, quality_uri))
        g.add((quality_uri, RDF.type, CCO.Quality))
        g.add((quality_uri, RDF.type, BFO.SpecificallyDependentContinuant))
        g.add((quality_uri, RDFS.label, Literal(f"{row['artifact_id']} {quality_kind} quality")))
        qualities_seen.add(quality_uri)

    # Measurement
    g.add((quality_uri, CCO.has_measurement, measurement_uri))
    g.add((measurement_uri, RDF.type, CCO.Measurement))
    g.add((measurement_uri, RDFS.label, Literal(f"{row['artifact_id']} {quality_kind} measurement {idx}")))
    g.add((measurement_uri, CCO.has_data_value, Literal(value, datatype=XSD.float)))
    g.add((measurement_uri, CCO.measurement_time,
           Literal(tstamp.isoformat(), datatype=XSD.dateTime)))

    # Unit (add type and label, avoid duplicates)
    g.add((measurement_uri, CCO.has_unit, unit_uri))
    if (unit_uri, RDF.type, CCO.MeasurementUnit) not in g:
        g.add((unit_uri, RDF.type, CCO.MeasurementUnit))
        # Add human-readable labels for units
        unit_labels = {
            "degC": "degree Celsius",
            "degF": "degree Fahrenheit",
            "kPa_gauge": "kilopascal gauge",
            "V": "volt",
            "Ω": "ohm",
            "ohm": "ohm"
        }
        label = unit_labels.get(unit, unit)
        g.add((unit_uri, RDFS.label, Literal(label)))
        
        # Add alternative labels for special units
        if unit == "Ω" or unit == "ohm":
            g.add((unit_uri, SKOS.altLabel, Literal("Ω", lang="en")))
            g.add((unit_uri, SKOS.altLabel, Literal("ohms", lang="en")))

# ---- Save TTL ----
output_file.parent.mkdir(parents=True, exist_ok=True)
g.serialize(destination=str(output_file), format="turtle")

print(f"✓ TTL successfully written to:\n{output_file}")
print(f"✓ Artifacts: {len(artifacts_seen)}")
print(f"✓ Qualities: {len(qualities_seen)}")
print(f"✓ Measurements: {len(df)}")