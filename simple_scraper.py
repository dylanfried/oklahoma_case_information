from bs4 import BeautifulSoup
import urllib2
import csv
import re

# List of all counties to loop over
counties = ['oklahoma']
# List of all years to loop over
years = [2015]
# Keep track of data
counts_data = []
dockets_data = []

for county in counties:
    print "Searching county: %s" % (county,)
    for year in years:
        print "Searching year: %s" % (year,)
        # For every county - year combo, start our number counter back at 1
        number = 1
        # Loop infinitely until we hit a stop page
        while True:
            print "Searching number: %s" % (number,)
            # Generate the url
            url = "http://www.oscn.net/dockets/GetCaseInformation.aspx?db=%s&number=cf-%d-%d" % (county, year, number)
            # Grab the html response
            response = urllib2.urlopen(url).read()
            # Use beautiful soup (https://www.crummy.com/software/BeautifulSoup/bs4/doc/) to parse the html response
            soup = BeautifulSoup(response, 'html.parser')
            # Check to see if we've hit the end, if so, break out
            if soup.findAll(text=re.compile('No Case Found')):
                break
            # Extract the data that we want out of the beautiful soup
            # Extract counts data
            for div in soup.find_all('div', class_='CountsContainer'):
                # Keep a dictionary for each count item
                count = {}
                count['County'] = county
                count['Year'] = year
                count['Number'] = number
                count['URL'] = url
                count['Party Name'] = div.find_all('table')[1].find_all('td')[1].string
                # Strip all whitespace out of the disposition information before saving it
                count['Disposition Information'] = ' '.join(' '.join([s for s in div.find_all('table')[1].find_all('td')[2].stripped_strings]).split())
                # Strip all whitespace out of the description before saving it
                count['Description'] = ' '.join(' '.join([s for s in div.find_all('table')[0].find_all('td')[1].stripped_strings]).split())
                counts_data.append(count)
            # You can do something similar to extract the data for the other table here
            number += 1

            # Uncomment this out to test with a small number
            # if number >= 10:
            #     break
# Write to CSV files
# Write counts CSV file
with open('counts.csv', 'w') as csvfile:
    counts_writer = csv.DictWriter(csvfile, fieldnames=counts_data[0].keys())
    counts_writer.writeheader()
    counts_writer.writerows(counts_data)
# Write docker CSV file
# with open('dockets.csv', 'w') as csvfile:
#     dockets_writer = csv.DictWriter(csvfile, fieldnames=dockets_data[0].keys())
#     dockets_writer.writeheader()
#     dockets_writer.writerows(dockets_data)
