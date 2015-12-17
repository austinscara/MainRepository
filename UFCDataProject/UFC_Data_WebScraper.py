import requests 
import html5lib
import csv
from bs4 import BeautifulSoup

#This is a change

"""
This Modules is scraping for links that contain fightevent information
it will grab links and read it in to a list

That list will then be read to loop through the fights and individual bouts
"""

# The link to all the fight information
# http://www.fightmetric.com/statistics/events/completed?page=all
# Date Range = no limit


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

    # Prouces a list of dictionaries {a unique Identifyier: "Name of Fight", link to fight, date of fight, Location of the fight}
    fightList_fightName = [{fightLinks.index(i): [i.get_text().strip(), i['href'] ,next(fightDates), next(fightLoc)]} for i in fightLinks]
    
    # flattens fightList_fightName to a dictionary{unique_id : [name, link, date, Location]}
    # Is this going to stay sorted?????
    id_Name_Link_Date_Loc = {key: value for dictionary in fightList_fightName for key, value in dictionary.items()}

    # Return dictionary of dictionary{unique_id : [name, link, date, Location]}
    return id_Name_Link_Date_Loc



def scrapeEventDetials(events):
    fight_Events_Info = []
    name_Link_Fighters = [] 
    
    # For every fight event
    for eventLink in events:
        website = requests.get(events[eventLink][1]).content  
        soup = BeautifulSoup(website, 'html5lib').body
########################################        
        # See Var Names
        fight_Name = soup.find('span', {'class': 'b-content__title-highlight'}).get_text().strip()
        fight_Attendance = soup.find('ul', {'class':'b-list__box-list'}).find_all('li')[-1].get_text().strip().split()
        if len(fight_Attendance) == 1:
            # If no attendance then populate with NULL
            basic_Fight_info = [fight_Name, "NULL"]
        else:
            # populates with attendance
            basic_Fight_info = [fight_Name, fight_Attendance[1]]
        # Adds list to final list    
        fight_Events_Info.append(basic_Fight_info)
########################################
        
        events_Fight = soup.find('tbody').find_all('tr')
        # for each fight in the fight event see vars
        for row in events_Fight:
            fight_Link = row['data-link']
            fighter_One = row.find('td', {'class': 'b-fight-details__table-col l-page_align_left'}).find_all('p')[0].get_text().strip()
            fighter_Two = row.find('td', {'class': 'b-fight-details__table-col l-page_align_left'}).find_all('p')[1].get_text().strip()            
            name_Link_Fighters.append([fight_Name, fight_Link, fighter_One, fighter_Two])
    # returns [fight name, fight attendance], [fight_Name, fight_Link, fighter_One, fighter_Two]
    return fight_Events_Info, name_Link_Fighters




allEventsURL = 'http://www.fightmetric.com/statistics/events/completed?page=all'

csvDictionary = {'fightMetric_Events': r'C:\Users\Austi\Documents\UFCDataProject\UFC_Data_CSV_Files\FightMetric_Events.csv',
                 'fightMetric_Events_info': r'C:\Users\Austi\Documents\UFCDataProject\UFC_Data_CSV_Files\FightMetric_Events_info.csv',
                 'fightMetric_Events_Name_link_fighter': r'C:\Users\Austi\Documents\UFCDataProject\UFC_Data_CSV_Files\FightMetric_Name_link_fighters.csv'}


headerDictionary = {'fightMetric_Events' : ['Fight Title', 'Fight Link', 'Fight Date', 'Fight Location'], 
                    'fightMetric_Events_info': ['Fight Title', 'Attendance'],
                    'fightMetric_Events_Name_link_fighter': ['Fight Name', 'Fight Link', 'Fighter One', 'Fighter Two']}



# Gets dictionary of dictionary{unique_id : [name, link, date, Location]}
id_Name_Link_Date_Loc = scrapeFightEvents(allEventsURL)

# Writes to a file in csvDictionary with header in headerDictionary
csvWritter(id_Name_Link_Date_Loc.values(), csvDictionary['fightMetric_Events'], headerDictionary['fightMetric_Events'])

# Returns Basic Fight info [fight name, fight attendance], [name, link, fighter one, fighter two]
fightDetails = scrapeEventDetials(id_Name_Link_Date_Loc)

# Writes fight details to two separate csv's
csvWritter(fightDetails[0], csvDictionary['fightMetric_Events_info'], headerDictionary['fightMetric_Events_info'])
csvWritter(fightDetails[1], csvDictionary['fightMetric_Events_Name_link_fighter'], headerDictionary['fightMetric_Events_Name_link_fighter'])

print ("Script is Done")