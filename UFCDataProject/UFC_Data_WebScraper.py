import requests 
import html5lib
import csv
import os
import itertools
import time
from multiprocessing.dummy import Pool  # This is a thread-based Pool
from multiprocessing import cpu_count
from memory_profiler import profile
from bs4 import BeautifulSoup
from datetime import datetime

#This is a change

"""
This Modules is scraping for links that contain fightevent information
it will grab links and read it in to a list

That list will then be read to loop through the fights and individual bouts
"""

# The link to all the fight information
# http://www.fightmetric.com/statistics/events/completed?page=all
# Date Range = no limit

startScript = datetime.now()


def csvWritter(data, csvName, header):
    # Block used to data to csvName
    # With header
    with open(csvName, 'w') as csvLog:
        writer = csv.writer(csvLog, header, lineterminator = '\n') 
        writer.writerow(header)
        for eventData in data:
            writer.writerow(eventData)
    return None



def scrapeFightEvents(url): #Scrapes for all events on page 
    website = requests.get(url).content
    soup = BeautifulSoup(website, 'html5lib').body.find('tbody')
    # Finds Link and name of fight
    fightLinks = soup.find_all('a', href = True, text = True)
    # Finds Date of fight
    fightDates = soup.find_all('span')
    # Finds Location of fight
    fightLoc = soup.find_all('td', {'class' : 'b-statistics__table-col b-statistics__table-col_style_big-top-padding'})
    # Produces a generator of fight dates
    fightDates = (i.get_text().strip() for i in fightDates)
    # Prouces a generator of fight location
    fightLoc = (i.get_text().strip() for i in fightLoc)
    # Returns generator object yeilding a list: [fight Name, fight link, gen(fight Date), gen(fight location)]
    return ([i.get_text().strip(), i['href'] ,next(fightDates), next(fightLoc)] for i in fightLinks)


# @profile
def scrapeEventDetials(events):
    # For every fight event

    website = requests.get(events[1]).content   
    time.sleep(3)
    soup = BeautifulSoup(website, 'html5lib').body
    
    fight_Name = soup.find('span', {'class': 'b-content__title-highlight'}).get_text().strip()
    fight_Attendance = soup.find('ul', {'class':'b-list__box-list'}).find_all('li')[-1].get_text().strip().split()[-1]
    if fight_Attendance == 'Attendance:':
        fight_Attendance = 'NULL'

    eventDetails = [[fight_Name,
            row['data-link'],
            fight_Attendance,
            row.find('td', {'class': 'b-fight-details__table-col l-page_align_left'}).find_all('p')[0].get_text().strip(),
            row.find('td', {'class': 'b-fight-details__table-col l-page_align_left'}).find_all('p')[1].get_text().strip()]  
            for row in soup.find('tbody').find_all('tr')]
    return eventDetails

#Main Link To Scrape
allEventsURL = 'http://www.fightmetric.com/statistics/events/completed?page=all'

#Windows
csvDictionary = {'fightMetric_Events': r'C:\Users\Austi\Documents\GitHub\MainRepository\UFCDataProject\UFC_Data_CSV_Files\FightMetric_Events.csv',
                 'fightMetric_Events_Name_link_fighter': r'C:\Users\Austi\Documents\GitHub\MainRepository\UFCDataProject\UFC_Data_CSV_Files\FightMetric_Name_link_fighters.csv'}

# MAC
# csvDictionary = {'fightMetric_Events': r'/Users/austinscara/Documents/GitHub/MainRepository/UFCDataProject/UFC_Data_CSV_Files/FightMetric_Events.csv',
#                  'fightMetric_Events_Name_link_fighter': r'/Users/austinscara/Documents/GitHub/MainRepository/UFCDataProject/UFC_Data_CSV_Files/FightMetric_Name_link_fighters.csv'}

headerDictionary = {'fightMetric_Events' : ['Fight Title', 'Fight Link', 'Fight Date', 'Fight Location'], 
                    'fightMetric_Events_Name_link_fighter': ['Fight Name', 'Fight Link', 'Attendance','Fighter One', 'Fighter Two']}

# Writes to a file in csvDictionary with header in headerDictionary
csvWritter(scrapeFightEvents(allEventsURL), csvDictionary['fightMetric_Events'], headerDictionary['fightMetric_Events'])

############################
# Threading implementation: 
# 4 threads = 4:36
# 8 threads = 3:30
############################

# Sets up thread Pool
pool = Pool(8)
# Maps items across threads 
results = pool.imap(scrapeEventDetials, scrapeFightEvents(allEventsURL))
# Creates a list of lists formatted for csvWritter
toWrite = [j for i in results for j in i]
# Writes Event details to CSV
csvWritter(toWrite, csvDictionary['fightMetric_Events_Name_link_fighter'], headerDictionary['fightMetric_Events_Name_link_fighter'])

print ("scrpit took: ", datetime.now() - startScript )