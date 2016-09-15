from bs4 import BeautifulSoup
import urllib2
import csv
import re
import time
import Queue
import threading

# Keep track of data
counts_data = {
    #'oklahoma': {
        #2011: [],
        #2015:[]
    #},
    'tulsa': {
       #2011:[],
       2015:[]
    }
}

max_number = {
    'oklahoma': {
        2011: 7168,
        2015:9178
    },
    'tulsa': {
        2011:4980,
        2015:6928
    }
}

def read_url(url, queue, county, year, number):
    response = None
    try:
        response = urllib2.urlopen(url).read()
    except:
        try:
            time.sleep(5)
            response = urllib2.urlopen(url).read()
        except:
            try:
                time.sleep(10)
                response = urllib2.urlopen(url).read()
            except:
                print "Could not retrieve data for url: %s" % (url,)
    if queue.qsize() % 100 == 0:
        print "Queue size: %d" % (queue.qsize(),)
    queue.put((response,url, county, year, number))

result = Queue.Queue()
for county in counts_data.keys():
    for year in counts_data[county].keys():
        threads = []
        for number in range(1,max_number[county][year] + 1):
            url = "http://www.oscn.net/dockets/GetCaseInformation.aspx?db=%s&number=cf-%d-%d" % (county, year, number)
            threads.append(threading.Thread(target=read_url, args = (url,result, county, year, number)))
            # if number >= 50:
            #     break
            if number % 100 == 0:
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()
                threads = []
        for t in threads:
            t.start()
        for t in threads:
            t.join()

print "Done retreiving HTML data"

# Done retrieving HTML data
# Process it
error_count = 0

while not result.empty():
    if result.qsize() % 100 == 0:
        print "Queue size: %d" % (result.qsize(),)
    item = result.get(block=False)
    # Check to make sure that we successfully fetched url
    if not item[0]:
        error_count += 1
        continue
     # Use beautiful soup (https://www.crummy.com/software/BeautifulSoup/bs4/doc/) to parse the html response
    soup = BeautifulSoup(item[0], 'html.parser')
    # Extract the data that we want out of the beautiful soup
    # Extract counts data
    # Loop over every counts container div. Each of these contains a single count, which may include multiple
    # dispositions
    for div in soup.find_all('div', class_='CountsContainer'):
        # Loop over each row in the table that contains the disposition(s) for the count
        for tr in div.find_all('table')[1].find_all('tbody')[0].find_all('tr'):
            # Keep a dictionary for each disposition item
            disposition = {}
            disposition['URL'] = item[1]
            disposition['County'] = item[2]
            disposition['Year'] = item[3]
            disposition['CaseNo'] = item[4]
            # Grab the case number by grabbing the correct `td`, stripping off 'Count # ' from the beggining and '.' from the end
            try:
                disposition['Count No'] = [s for s in div.find_all('table')[0].find_all('td')[0].stripped_strings][0][8:-1]
            except:
                print "Could not find 'Count No' for county: %s, year: %s, url: %s" % (item[2], item[3], item[1])
            try:
                disposition['Party Name'] = tr.find_all('td')[1].string
            except:
                print "Could not find 'Party Name' for county: %s, year: %s, url: %s" % (item[2], item[3], item[1])
            try:
                disposition['Count as Filed'] = ' '.join(' '.join([s for s in div.find_all('table')[0].find_all('td')[1].stripped_strings][:2]).split())[16:]
            except:
                print "Could not find 'Count as Filed' for county: %s, year: %s, url: %s" % (item[2], item[3], item[1])
            try: 
                disposition['Date of Offense'] = [s for s in div.find_all('table')[0].find_all('td')[1].stripped_strings][2][17:]
            except:
                print "Could not find 'Date of Offense' for county: %s, year: %s, url: %s" % (item[2], item[3], item[1])
            try: 
                disposition['Disposition'] = ' '.join(tr.find_all('td')[2].find_all('strong')[0].string.split())[10:]
            except:
                print "Could not find 'Disposition' for county: %s, year: %s, url: %s" % (item[2], item[3], item[1])
            try:
                disposition['Count as Disposed'] = [s for s in tr.find_all('td')[2].stripped_strings][1][19:]
            except:
                print "Could not find 'Count as Disposed' for county: %s, year: %s, url: %s" % (item[2], item[3], item[1])
            counts_data[item[2]][item[3]].append(disposition)

# Write to CSV files
# Write counts CSV file
for county in counts_data.keys():
    for year in counts_data[county].keys():
        with open('counts_%s_%s.csv' % (county, year), 'w') as csvfile:
            counts_writer = csv.DictWriter(csvfile, fieldnames=['URL','County','Year','CaseNo', 'Count No', 'Count as Filed', 'Date of Offense', 'Party Name', 'Disposition', 'Count as Disposed'])
            counts_writer.writeheader()
            for row in counts_data[county][year]:
                counts_writer.writerow({k:unicode(v).encode('utf-8') for k,v in row.items()})

print "Unable to retrieve: %d cases" % (error_count,)

