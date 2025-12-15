import pandas as pd
import sys
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD
from dateutil import parser as dateparser
import re
from pathlib import Path

# ---- Define paths relative to this script ----
SCRIPT_DIR = Path(__file__).resolve().parent  # .../src/scripts
SRC_DIR = SCRIPT_DIR.parent  # .../src

csv_file = SRC_DIR / "data" / "readings_normalized.csv"
output_file = SRC_DIR / "measure_cco.ttl"

# ---- Load your measurement data from CSV ----
print(f"[measure_rdflib] Loading data from: {csv_file}")

if not csv_file.exists():
    print(f"ERROR: {csv_file} not found. Run normalize_readings.py first!")
    sys.exit(1)

df = pd.read_csv(csv_file)
print(f"[measure_rdflib] Loaded {len(df)} measurements")

# ---- Namespaces - Using EXACT IRIs required by ETL ----
EX = Namespace("http://example.org/")
CCO = Namespace("https://www.commoncoreontologies.org/")
BFO = Namespace("http://purl.obolibrary.org/obo/")

# Exact class IRIs
IRI_ARTIFACT = URIRef("https://www.commoncoreontologies.org/ont00000995")
IRI_SDC = URIRef("http://purl.obolibrary.org/obo/BFO_0000020")
IRI_MICE = URIRef("https://www.commoncoreontologies.org/ont00001163")
IRI_MU = URIRef("https://www.commoncoreontologies.org/ont00000120")

# Exact property IRIs
IRI_BEARER_OF = URIRef("http://purl.obolibrary.org/obo/BFO_0000196")
IRI_IS_MEASURE_OF = URIRef("https://www.commoncoreontologies.org/ont00001966")
IRI_USES_MU = URIRef("https://www.commoncoreontologies.org/ont00001863")

# ---- RDF Graph ----
g = Graph()
g.bind("ex", EX)
g.bind("cco", CCO)
g.bind("bfo", BFO)

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

    # Artifact (only add once) - Use exact IRI
    if artifact_uri not in artifacts_seen:
        g.add((artifact_uri, RDF.type, IRI_ARTIFACT))
        g.add((artifact_uri, RDFS.label, Literal(row["artifact_id"])))
        artifacts_seen.add(artifact_uri)

    # Quality/SDC (only add once per artifact-quality pair) - Use exact IRIs
    if quality_uri not in qualities_seen:
        g.add((artifact_uri, IRI_BEARER_OF, quality_uri))
        g.add((quality_uri, RDF.type, IRI_SDC))
        g.add((quality_uri, RDFS.label, Literal(f"{row['artifact_id']} {quality_kind} quality")))
        qualities_seen.add(quality_uri)

    # Measurement (MICE) - Use exact IRIs
    g.add((measurement_uri, RDF.type, IRI_MICE))
    g.add((measurement_uri, RDFS.label, Literal(f"{row['artifact_id']} {quality_kind} measurement {idx}")))
    g.add((measurement_uri, IRI_IS_MEASURE_OF, quality_uri))
    g.add((measurement_uri, CCO.has_data_value, Literal(value, datatype=XSD.decimal)))
    g.add((measurement_uri, CCO.measurement_time, Literal(tstamp.isoformat(), datatype=XSD.dateTime)))

    # Unit - Use exact IRI
    g.add((measurement_uri, IRI_USES_MU, unit_uri))
    if (unit_uri, RDF.type, IRI_MU) not in g:
        g.add((unit_uri, RDF.type, IRI_MU))
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

# ---- Save TTL ----
output_file.parent.mkdir(parents=True, exist_ok=True)
g.serialize(destination=str(output_file), format="turtle")

print(f"✓ TTL successfully written to:\n{output_file}")
print(f"✓ Artifacts: {len(artifacts_seen)}")
print(f"✓ Qualities: {len(qualities_seen)}")
print(f"✓ Measurements: {len(df)}")