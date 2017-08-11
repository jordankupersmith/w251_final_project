


import os
from bs4 import BeautifulSoup
import requests
import datetime
import wget
import time
import os.path

#sample path
#https://dumps.wikimedia.org/other/pageviews/2017/2017-07/pageviews-20170801-000000.gz 

#base_url='https://dumps.wikimedia.org/other/pageviews/2017/2017-07'

base_url='https://dumps.wikimedia.org/other/pageviews'

os.chdir('/data/')

##how many days do you want to go back?


today = datetime.date.today()
startday = datetime.date(2015, 05, 01)
diff = today - startday
numdays=diff.days

## should we start downloading from the beginning, or the end?
bStartAtBeginning = True

##What file extension are you trying to get?    
ext = 'gz'

###Define a function to get the file names within a directory
def listFD(url, ext=''):
    page = requests.get(url).text
    #print (page)
    soup = BeautifulSoup(page, 'html.parser')
    return [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]


###Get the dates you are trying to retrieve
if bStartAtBeginning:
    base = startday
    date_list = [base + datetime.timedelta(days=x) for x in range(0, numdays)]
else:
    base = datetime.date.today()
    date_list = [base - datetime.timedelta(days=x) for x in range(0, numdays)]

###make directories
months=[]
for r in range(1, 13):
    month="0" + str(r)
    months.append(month)


for year in range(2015, 2018):
    if os.path.isdir(str(year)):
        print year, "directory exists"
    else:
        os.mkdir(str(year))
        print year, "directory created"
    os.chdir(str(year))
    for month in months:
        month_dir= "%s-%s" % (year,month)
        if os.path.isdir(month_dir):
            print month_dir, "directory exists"
        else:
            os.mkdir(month_dir)
            print month_dir, "directory created"
    os.chdir('..')
            



##build the url names based on your dates

for d in date_list:
    month=d.strftime('%m')
    year=str(d.year)
    day=d.strftime('%d')
    url=base_url + '/' + year + '/' + year + '-' + month
    #print url

    ##iterate through your urls and list paths
    for file in listFD(url, ext):
        
        extract = file.split('-')[-1]
        hour=extract[:2]
        filenameshort = file.split('/')[-1]
        path='/data/' + year + '/' + year + '-' + month 
        os.chdir(path)
        print os.getcwd()
        if os.path.exists(filenameshort):
            print filenameshort, "is already downloaded"
        else:
            
            if "pageviews" in filenameshort:    
                time.sleep( 5 ) ## don't hit the servers too hard, wiki rocks so be nice :)
                start_time = time.time()     
                wget.download(file)
                print file
                end_time=time.time() - start_time
                
                filesize=os.path.getsize(filenameshort) >> 20
                print "The file ", filenameshort, "had a download speed of",  filesize / end_time , "MB per second"
            else:
                pass
        os.chdir('/data/')

