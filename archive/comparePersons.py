from collections import defaultdict
import csv
import itertools
import unicodedata
import re
import Levenshtein
import itertools


def normalize_name(name):
    # Normalize the name to NFC (Normalization Form C)
    name = unicodedata.normalize('NFC', name)
    
    # Convert to lowercase
    name = name.lower()
    
    # Normalize unicode and remove diacritics
    name = ''.join(
        c for c in unicodedata.normalize('NFD', name)
        if unicodedata.category(c) != 'Mn'
    )
    
    # Remove any characters that aren't letters (no spaces, no numbers)
    name = re.sub(r'[^a-zA-Z]', '', name)
    
    return name



def readCsv(csv_file) :
    # Create a dictionary, where key is a normalized name and value is the list of IRIs
    # For example, {'johndoe': ["https://example.org/person1", "https://example.org/person2" ]}
    data = defaultdict(lambda: defaultdict(list))
    with open(csv_file, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            normalized_name = normalize_name(row['name'])
            year = row['year']
            if not row['person'] in data[normalized_name]['iri']:
                data[normalized_name]['iri'].append(row['person'])
            data[normalized_name]['name'] = normalized_name
            data[normalized_name]['year'] = year
   
    return data

def writeTriples(filename, list1, list2):
        with open(filename, 'a') as f:

            for element in itertools.product(list1, list2):
                f.write(f" <{element[0]}> <http://www.w3.org/2002/07/owl#sameAs> <{element[1]}> .\n" )
                f.write(f" <{element[1]}> <http://www.w3.org/2002/07/owl#sameAs> <{element[0]}> .\n" )


def fileLength(filename):
    # Check the current number of matches
    with open(filename,"r") as f:
        return len(f.readlines())
    
def writingCSV(filename, matching_terms):
    with open(filename, mode="w", encoding="utf-8", newline="") as output_file:
        fieldnames = ["Term in CSV", "Other Term in CSV", "Percentage Match", "Muziekschatten", "MuziekWeb"]
        writer = csv.DictWriter(output_file, fieldnames=fieldnames, delimiter= "|")
        writer.writeheader()

        for match in matching_terms:
            term1, term2, percentage_match , iri1, iri2= match
            writer.writerow({
                "Term in CSV": term1,
                "Other Term in CSV": term2,
                "Percentage Match": percentage_match,
                "Muziekschatten":iri1,
                "MuziekWeb": iri2                

            })
    
ttl_LV_match = 'matchedPersonNameYearLV-combined.nt'
ttl_name_match = 'matchedPersonCombined.nt'
csv_LV_match = 'levenshteinMatchNameYear_combined.csv'

person_muziekweb = readCsv('static/Muziekweb.csv')
# more_than_one = {key: values['iri'] for key, values in person_muziekweb.items() if len(values['iri'])>1} 
print(f"Muziekweb Iris: {len({item for values in person_muziekweb.values() for item in values['iri']})}")

person_muziekschatten = readCsv('static/Muziekschatten.csv')
print(f"Muziekschatten Iris: {len({item for values in person_muziekschatten.values() for item in values['iri'] })}")

exactMatch = set()
# Add exact matches for normalized names 
for name, values in person_muziekschatten.items():
    if name in person_muziekweb:
        muziekwebIris = person_muziekweb[name]['iri']
        writeTriples(ttl_name_match,values['iri'], muziekwebIris)
        exactMatch.add(name)

print(f"The file {ttl_name_match} currently has  {fileLength(ttl_name_match)} triples")

print(len(exactMatch))
min_percentage_match = 80
matching_terms = []



# # Add matches based on fuzzy matching 
for name1, values1 in  person_muziekschatten.items():
    if name1 not in exactMatch and len(values1['name'])>=8:
        for name2, values2 in person_muziekweb.items():
             if name2 not in exactMatch and len(values2['name'])>=8:
                if values1['year'] == values2['year'] and  values1['year'] != '' and values2['year']!= '':
                    distance = Levenshtein.distance(values1['name'] , values2['name'])
                    max_length = max(len(values1['name'] ), len(values2['name']))
                    percentage_match = (1 - distance / max_length) * 100

                    # Check if the percentage match is above the threshold
                    if percentage_match >= min_percentage_match:
                        
                        matching_terms.append((values1['name'] , values2['name'], percentage_match, '\n'.join(values1['iri']), '\n'.join(values2['iri']) ))
                        writeTriples(ttl_LV_match, values1['iri'],values2['iri'])


writingCSV(csv_LV_match, matching_terms)








