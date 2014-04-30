import csv

# Load the house price data.
# Source: http://www.landregistry.gov.uk/public/information/public-data/price-paid-data/download
prices = {}
averages = {}

with open('data/house-prices.csv', 'rb') as f:
    reader = csv.reader(f)
    for row in reader:
        postcode = row[3].split(' ')[0]
        price = row[1]

        if postcode not in prices:
            prices[postcode] = []

        prices[postcode].append(int(price))

# Load the postcode data.
# Source: http://www.doogal.co.uk/PostcodeDistricts.php
with open('data/postcodes.csv', 'rb') as f:
    reader = csv.reader(f)
    for row in reader:
        if row[0] in prices.keys():
            pc_prices = prices[row[0]]

            averages[row[0]] = {
                'average': sum(pc_prices) / float(len(pc_prices)),
                'lat': row[1],
                'lon': row[2]
            }

# Write out the averages
writer = csv.writer(open('data.csv', 'wb'))
writer.writerow(['Postcode', 'Lat', 'Lon', 'Average Price'])
for key, value in averages.items():
    writer.writerow([key, value['lat'], value['lon'], value['average']])

