import requests 
import html5lib
import csv
import os
import itertools
import time
import threading
from queue import Queue
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
exitFlag = 0

class myThread (threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
    def run(self):
        print ("Starting " + self.name)
        scrapeEventDetials(self.name, self.q)
        print ("Exiting " + self.name)


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
    # for i in fightLinks:
    #   yield [i.get_text().strip(), i['href'] ,next(fightDates), next(fightLoc)] 
    return ([i.get_text().strip(), i['href'] ,next(fightDates), next(fightLoc)] for i in fightLinks)


# @profile
def scrapeEventDetials(events, q):
    # For every fight event
    # print ("getting " + events + " data")
    
    while not exitFlag:
        if not workQueue.empty():
            data = q.get()

            website = requests.get(events).content
            soup = BeautifulSoup(website, 'html5lib').body
            time.sleep(3)
            fight_Name = soup.find('span', {'class': 'b-content__title-highlight'})#.get_text().strip()

            print (fight_Name)
            # fight_Attendance = soup.find('ul', {'class':'b-list__box-list'}).find_all('li')[-1].get_text().strip().split()[-1]   
            # # returns [fight name, fight Attendance], [fight_Name, fight_Link, fighter_One, fighter_Two]
            # gen = [[fight_Name,
            #         row['data-link'],
            #         fight_Attendance,
            #         row.find('td', {'class': 'b-fight-details__table-col l-page_align_left'}).find_all('p')[0].get_text().strip(),
            #         row.find('td', {'class': 'b-fight-details__table-col l-page_align_left'}).find_all('p')[1].get_text().strip()]  
            #         for row in soup.find('tbody').find_all('tr')]

            queueLock.release()
            print ("%s processing %s" % (threadName, data))
        else:
            queueLock.release()
        time.sleep(1)

    # website = requests.get(events).content
    # soup = BeautifulSoup(website, 'html5lib').body
    # time.sleep(3)
    # fight_Name = soup.find('span', {'class': 'b-content__title-highlight'})#.get_text().strip()

    # print (fight_Name)
    # # fight_Attendance = soup.find('ul', {'class':'b-list__box-list'}).find_all('li')[-1].get_text().strip().split()[-1]   
    # # # returns [fight name, fight Attendance], [fight_Name, fight_Link, fighter_One, fighter_Two]
    # # gen = [[fight_Name,
    # #         row['data-link'],
    # #         fight_Attendance,
    # #         row.find('td', {'class': 'b-fight-details__table-col l-page_align_left'}).find_all('p')[0].get_text().strip(),
    # #         row.find('td', {'class': 'b-fight-details__table-col l-page_align_left'}).find_all('p')[1].get_text().strip()]  
    # #         for row in soup.find('tbody').find_all('tr')]



allEventsURL = 'http://www.fightmetric.com/statistics/events/completed?page=all'



#Windows
# csvDictionary = {'fightMetric_Events': r'C:\Users\Austi\Documents\GitHub\MainRepository\UFCDataProject\UFC_Data_CSV_Files\FightMetric_Events.csv',
#                  'fightMetric_Events_info': r'C:\Users\Austi\Documents\GitHub\MainRepository\UFCDataProject\UFC_Data_CSV_Files\FightMetric_Events_info.csv',
#                  'fightMetric_Events_Name_link_fighter': r'C:\Users\Austi\Documents\GitHub\MainRepository\UFCDataProject\UFC_Data_CSV_Files\FightMetric_Name_link_fighters.csv'}


# MAC
csvDictionary = {'fightMetric_Events': r'/Users/austinscara/Documents/GitHub/MainRepository/UFCDataProject/UFC_Data_CSV_Files/FightMetric_Events.csv',
                 'fightMetric_Events_info': r'/Users/austinscara/Documents/GitHub/MainRepository/UFCDataProject/UFC_Data_CSV_Files/FightMetric_Events_info.csv',
                 'fightMetric_Events_Name_link_fighter': r'/Users/austinscara/Documents/GitHub/MainRepository/UFCDataProject/UFC_Data_CSV_Files/FightMetric_Name_link_fighters.csv'}



headerDictionary = {'fightMetric_Events' : ['Fight Title', 'Fight Link', 'Fight Date', 'Fight Location'], 
                    'fightMetric_Events_info': ['Fight Title', 'Attendance'],
                    'fightMetric_Events_Name_link_fighter': ['Fight Name', 'Fight Link', 'Fighter One', 'Fighter Two']}




# Writes to a file in csvDictionary with header in headerDictionary
csvWritter(scrapeFightEvents(allEventsURL), csvDictionary['fightMetric_Events'], headerDictionary['fightMetric_Events'])

############################
# for i in scrapeFightEvents(allEventsURL):
#     print (scrapeEventDetials(i[1]))


# Returns Basic Fight info [fight name, fight Attendance], [name, link, fighter one, fighter two]
threadList = scrapeFightEvents(allEventsURL)
nameList = scrapeFightEvents(allEventsURL)
queueLock = threading.Lock()
workQueue = Queue()
threads = []
threadID = 1


# Create new threads
for tName in threadList:
    thread = myThread(threadID, tName[1], workQueue)
    thread.start()
    threads.append(thread)
    threadID += 1

# Fill the queue
queueLock.acquire()
for word in nameList:
    workQueue.put(word)
queueLock.release()

# Wait for queue to empty
while not workQueue.empty():
    pass

# Notify threads it's time to exit
exitFlag = 1

# Wait for all threads to complete
for t in threads:
    t.join()
print ("Exiting Main Thread")



# fightDetails = scrapeEventDetials(scrapeFightEvents(allEventsURL))


# # Writes fight details to two separate csv's
# csvWritter(fightDetails[0], csvDictionary['fightMetric_Events_info'], headerDictionary['fightMetric_Events_info'])
# csvWritter(fightDetails[1], csvDictionary['fightMetric_Events_Name_link_fighter'], headerDictionary['fightMetric_Events_Name_link_fighter'])

print ("scrpit took: ", datetime.now() - startScript )
print (os.cpu_count())




