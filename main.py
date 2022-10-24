import json
import re

import pyspark as pyspark
import requests
from bs4 import BeautifulSoup


urls = []
finaloutputlist = []


# function to add the token value dynamically to the URL
def urlextra(jid):
    temp = 'https://boards.greenhouse.io/embed/job_app?for=coursera&token={}'
    url = temp.format(jid)
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    return soup


# function to add all the required values Token, locatin, Job Title, Job overview to a dictionary and store it to a list.
def adddic(urls):
    for c in urls:
        dictn = {'tokenid': c}  # adding the tokenid to the dictionary
        soup = urlextra(c)
        totaldiv = soup.find_all('div', class_="location")
        for i in totaldiv:
            dictn['country'] = i.get_text().strip()  # Code to extract the data of the job location

        # code to extract the title from the job page.
        totaldiv = soup.find_all('h1')
        for i in totaldiv:
            dictn['title'] = i.get_text()

        # Code to extract the Job overview data from the Job pge
        totaldiv = soup.find_all("p")
        for i in totaldiv:
            if i.get_text() == "Job Overview:" or i.get_text() == "Job Overview":  # if data of the paragraph equals to Job Overview, then we can fetch the data of the next paragraph.
                dictn['Job Overview:'] = i.find_next('p').get_text()

        # code to get all the required Basic qualifications for a job.
        totaldiv = soup.find_all("p")
        for i in totaldiv:
            if i.get_text() == "Basic Qualifications:" or i.get_text() == "Basic Qualifications":  # if data of the paragraph equals to Job Overview, then we can fetch the data of the next paragraph.
                temp1 = i.find_next('ul').find_all('li')
                templist=[]
                for j in temp1:
                    templist.append(j.get_text())
                dictn['Basic Qualifications']=templist




        finaloutputlist.append(dictn)



URL = "https://about.coursera.org/careers/jobs/"

r = requests.get(URL)
print(r.text)
soup = BeautifulSoup(r.text, 'html.parser')
totaldiv = soup.findAll('div', class_="opening")

# Using a re to get the token id present in the link/url and storing all the tokenid's to a list.
for div in totaldiv:
    x = div.a.get("href")
    temp = re.findall(r'\d+', x)
    res = list(map(int, temp))
    urls.append(res[0])

adddic(urls)
print(finaloutputlist)
