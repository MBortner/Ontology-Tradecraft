import pandas as pd
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, XSD
from dateutil import parser as dateparser
import re
from pathlib import Path

# ---- Output Path ----
output_file = Path("/Users/MikeBortner/Documents/GitHub/Repos/Ontology-Tradecraft/projects/project-4/assignment/src/measure_cco.ttl")

# ---- Namespaces ----
EX = Namespace("http://example.org/")
CCO = Namespace("http://www.ontologyrepository.com/CommonCoreOntologies/")
BFO = Namespace("http://purl.obolibrary.org/obo/BFO_")

# ---- Load your measurement data ----
df = pd.DataFrame([
    ["Boiler-07","temperature","degC",100.0,"2024-03-17T23:03:00Z"],
    ["Boiler-07","pressure","kPa_gauge",101.325,"2024-03-17T23:04:00Z"],
    ["Chiller-3","temperature","degC",19.5,"2024-03-18T12:01:00Z"],
    ["Chiller-4","temperature","degF",6.5,"2024-03-18T12:01:00Z"],
    ["Chiller-4","temperature","degF",19.5,"2024-03-18T12:06:00Z"],
    ["Circuit-12","voltage","V",2.0,"2024-03-18T12:01:01Z"],
    ["Circuit-12","resistance","Ω",1.3,"2024-03-18T12:06:00Z"],
], columns=["artifact_id","sdc_kind","unit_label","value","timestamp"])

# ---- RDF Graph ----
g = Graph()
g.bind("ex", EX)
g.bind("cco", CCO)
g.bind("bfo", BFO)

# ---- Helper for safe URI creation ----
def clean(x):
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(x))

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

    # Artifact
    g.add((artifact_uri, RDF.type, CCO.Artifact))

    # Quality
    g.add((artifact_uri, CCO.has_quality, quality_uri))
    g.add((quality_uri, RDF.type, CCO.Quality))

    # Measurement
    g.add((quality_uri, CCO.has_measurement, measurement_uri))
    g.add((measurement_uri, RDF.type, CCO.Measurement))
    g.add((measurement_uri, CCO.has_data_value, Literal(value, datatype=XSD.float)))
    g.add((measurement_uri, CCO.measurement_time,
           Literal(tstamp.isoformat(), datatype=XSD.dateTime)))

    # Unit
    g.add((measurement_uri, CCO.has_unit, unit_uri))
    g.add((unit_uri, RDF.type, CCO.MeasurementUnit))

# ---- Save TTL ----
output_file.parent.mkdir(parents=True, exist_ok=True)
g.serialize(destination=str(output_file), format="turtle")

print(f"TTL successfully written to:\n{output_file}")
