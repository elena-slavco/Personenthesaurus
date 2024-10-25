from rdflib import XSD, Graph, URIRef, Literal
import urllib.parse
import isodate

# Define the SPARQL endpoint
endpoint_url = "https://data.muziekschatten.nl/sparql"

# Define the SPARQL query
sparql_query = """
prefix schema: <http://schema.org/>
prefix vocab: <https://data.muziekweb.nl/vocab/>
prefix xsd: <http://www.w3.org/2001/XMLSchema#>
prefix skos: <http://www.w3.org/2004/02/skos/core#>
prefix som: <https://data.muziekschatten.nl/som/>
prefix owl: <http://www.w3.org/2002/07/owl#>
construct {
     ?person a schema:Person.
     ?person owl:sameAs ?link .
     ?person schema:birthDate ?birthDate.
     ?person som:GDAT  ?birthYear.
     ?person schema:name ?name.
     ?person schema:alternateName ?alternateName.
  } where {
      ?person a schema:Person;
              som:ZKNMFZ [] .
    ?person schema:name ?name.
      optional {
        ?person schema:birthDate ?birthDate.
      }
      optional {
        ?person som:GDAT  ?birthYear
      }    
      optional {
        ?person schema:alternateName ?alternateName
      }
      optional {?person owl:sameAs ?link}
}
"""
# Function to validate and clean URIs
def safe_uri(uri):
    # URL-encode any invalid characters
    return URIRef(urllib.parse.quote(uri, safe=':/#'))

# Function to safely handle datatypes, converting unrecognized formats to strings
def safe_literal(obj):
    try:
        # Attempt to parse ISO8601 dates
        if obj.datatype == XSD.dateTime or obj.datatype == XSD.date:
            # Parse the date to see if it's valid
            isodate.parse_datetime(str(obj))
        return obj
    except (isodate.isoerror.ISO8601Error, TypeError, ValueError):
        # If parsing fails, return as a simple string
        return Literal(str(obj), datatype=XSD.string)

# Variables for pagination
ttl_file_path = 'testdata.ttl'
limit = 10000
offset = 0
all_results = []

first_write = True

# Persistent RDFLib graph that accumulates all results
persistent_graph = Graph()

# Loop to fetch all results using pagination
while True:
    # Create the query with LIMIT and OFFSET
    paginated_query = f"{sparql_query} LIMIT {limit} OFFSET {offset}"
    encoded_query = urllib.parse.quote(paginated_query)
    
    # Construct the query URL
    query_url = f"{endpoint_url}?query={encoded_query}&format=xml"
    
    # Temporary graph to fetch this batch of results
    temp_graph = Graph()
    temp_graph.parse(query_url, format="xml")
    
    # If no results are returned, break the loop
    if len(temp_graph) == 0:
        break
    
      # Add results to the persistent graph, ensuring URIs are safe
    for subj, pred, obj in temp_graph:
        safe_subj = safe_uri(str(subj)) if isinstance(subj, URIRef) else subj
        safe_pred = safe_uri(str(pred)) if isinstance(pred, URIRef) else pred
        # Handle object datatypes
        if isinstance(obj, Literal):
            safe_obj = safe_literal(obj)
        elif isinstance(obj, URIRef):
            safe_obj = safe_uri(str(obj))
        else:
           safe_obj = obj
        persistent_graph.add((safe_subj, safe_pred, safe_obj))

    # Increment the offset for the next batch of results
    offset += limit


print(persistent_graph.serialize(ttl_file_path, 'ttl'))
    
  