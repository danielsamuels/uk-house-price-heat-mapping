import csv
import RDF
import re
import os

# First we want load the house price data and organise it as well as possible, to
# make geolocating as fast as possible later on.
# Data source: http://www.landregistry.gov.uk/public/information/public-data/price-paid-data/download
postcodes = {}

with open('data/house-prices.csv', 'rb') as f:
    postcode_regex = re.compile(r'(([A-Z]{1,2})[\dA-Z]{1,2}\ ?\d{1}[A-Z]{2})')
    reader = csv.reader(f)
    for row in reader:
        if not row[3]:
            continue;

        # We need the postcode in a few different formats:
        #
        # Postcode area: CB
        #  - To load the correct codepoint data file.
        # Full postcode: CB9 0AA
        #  - To get the lat lon for the specific postcode.

        # pm = postcode matches
        pm = postcode_regex.match(row[3]).groups()
        full_postcode = pm[0].replace(' ', '')

        # postcodes['CB']
        if pm[1] not in postcodes:
            postcodes[pm[1]] = {}

        # postcodes['CB']['CB9 0AA']
        if full_postcode not in postcodes[pm[1]]:
            postcodes[pm[1]][full_postcode] = {
                'prices': [],
                'lat': None,
                'lon': None,
            }

        postcodes[pm[1]][full_postcode]['prices'].append(int(row[1]))

# Iterate over each postcode region and geolocate the postcodes.
print '{} regions to process.'.format(len(postcodes.keys()))
for key in postcodes.keys():
    # Create an RDF parser
    parser = RDF.Parser(name="ntriples")
    model = RDF.Model()

    # Get a full path to the data files.
    file_path = os.path.abspath("data/codepoint/{}_position.nt".format(
        key.lower()
    ))

    # Open a file stream.
    print 'Opening stream for {}..'.format(key)
    stream = parser.parse_into_model(
        model,
        "file://{}".format(file_path),
        "http://data.ordnancesurvey.co.uk/id/postcodeunit/"
    )
    print 'Stream opened..'

    print 'Processing {}'.format(key)

    for triple in model:
        predicate = str(triple.predicate)
        postcode = str(triple.subject).replace('http://data.ordnancesurvey.co.uk/id/postcodeunit/', '')
        obj = str(triple.object)

        if postcode not in postcodes[key]:
            continue;

        if '#lat' in predicate:
            postcodes[key][postcode]['lat'] = obj
            print 'Processing {} - Set lat'.format(postcode)

        if '#lon' in predicate:
            postcodes[key][postcode]['lon'] = obj
            print 'Processing {} - Set lon'.format(postcode)

# Write out the averages
writer = csv.writer(open('data.csv', 'wb'))
writer.writerow(['Postcode', 'Lat', 'Lon', 'Average Price'])
for key, value in postcodes.items():
    for item_key, item_value in value.items():
        average = sum(item_value['prices']) / float(len(item_value['prices']))
        writer.writerow([item_key, item_value['lat'], item_value['lon'], average])

