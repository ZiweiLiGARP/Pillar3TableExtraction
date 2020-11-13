# -*- coding: utf-8 -*-
"""
This package contains functions to scrape reports from a given website
User can define the type of report to scrape (e.g. pdf)
and the subset of report to scrape by giving a list of keywords that must be
included in the filename

@main author: Ziwei Li
@author2: Ruoyi Ma
GBI - August 2020
"""

import requests
from bs4 import BeautifulSoup
import os
import glob
import pandas as pd
## if not given a path from use, use current work folder as output folder
dir_path = os.getcwd()

def get_filters(websitesPath,bank):
    '''
    load filter keywords from websites.xlsx 
    Use these keywords to filter and save only relevent documents
    '''
    
    try:
        websites=pd.read_excel(websitesPath,index_col=0)
    except PermissionError:
        print('Please close Websites.xlsx file and execute this command cell again')
        return False
        
    while True:
        try:
            filterList=websites.loc[bank,'Filter']
            
            if pd.isnull(filterList):
                print('No filter keywords detected, please check Row '+ bank+' and Column "Filter" of websites.xlsx file')
                print('\n')
                return False
            else:
                filterList= [s.strip().lower() for s in filterList.split(',')]
                print('Filter keywords: '+str(filterList))
                return (filterList)
            
        except KeyError:
            print('No filter keywords detected, please check Row '+ bank+' and Column "Filter" of websites.xlsx file')
            print('\n')
            return False


def download_file(link, name, path=''):
    '''
    download the expected pdf link to local: 
    link - pdf hyperlink; 
    name - name of the pdf file when it downloads; 
    path - appointed path if given, default as current folder
    '''

    r = requests.get(link)

    ## check if the given path already exists
    if not os.path.exists(path):
        ## if not create a folder for the path
        os.makedirs(path)
    
    ## create a new file named after 'name' at 'path'
    with open(path+name, 'wb') as outfile:
        ## write content
        outfile.write(r.content)


def get_all_file(name,filters, url, path, file_type='.pdf'):
    '''
    provide a link to the function and download all pdf files on the website to designated folder:
    name - name of the firm
    url - the website where it stores all pdf
    path - where to store the downloaded files
    '''
    print('Getting Files for '+name)

    ## probing for the url
    try:
        ## use timeout of 10 seconds to control the requests
        response = requests.get(url, timeout=10)
        ## status code 200 means that it succeed in grabing the website code
        if response.status_code == 200:
            print("Access Success")
        else:
            print("Access Failure: response code "+response.status_code)
    except:
        print('Access Denied')
        return None
    
    ## decode with beautiful soup package
    results_page = BeautifulSoup(response.content,'lxml')

    ## find all the hyperlink object within the page
    a_tag = results_page.find_all('a')
    ## take the root of the website, like https://www.citigroup.com
    root = 'https://' + url.split('//')[-1].split('/')[0]
    ## use the store path and the name of the firm as the subfolder for download
    dl_path = path + '\\' + name + '\\' + 'update' + '\\'
    ## create a list of all files in the check_path folder
    file_log = glob.glob(path + '\\' + name + '\\' + 'archived' + '\\'+'*.pdf')
    file_log = [x.split('\\')[-1] for x in file_log]
    ## create a variable to count new files
    update_count = 0

    ## loop through all the hyperlink objects from above
    for tag in a_tag:
        
        ## extract the link of object by getting the href attribute
        raw_link = tag.get('href')
        
        ## check if there is a link
        if raw_link==None:
            ## continue means skip this iteration and jump to the next object
            continue
        
        ## check whether the object is a pdf file
        if raw_link[-len(file_type):]!=file_type:
            continue
        
        ## this commented code was for citi to identify and filter out some non-english pdfs, but other banks are using it differently so block it for now
        '''
        title = tag.get('title')
        if title!=None:
            title = title.lower()
            if title.find('english')<0 and title.find('download')<0:
                continue
        '''

        ## take the last part of the pdf link as the name of the pdf
        pdf_name = raw_link.split('/')[-1]
        
        
        #if filters detected, only download the files which contain at least one of the keywords
        #if no filters detected, download all files from the given website
        match=False
        if filters!=False: #if we want to filter 
            for keywords in filters: #filter out files that match all keywords
                if (keywords in pdf_name.lower())==True:
                    match=True #as soon as we match a keyword
                    break #break the keywords loop, process this file
                else:
                    continue
        if match==False: #if a file doesn't match at least one of the keywords, skip this file
            continue

        ## check if this is an existing pdf in the check_path folder
        if pdf_name in file_log:
            continue
        
        ## multiple condition to identify which scenario the link belongs to
        if raw_link.startswith('http'):
            ## the link starts with 'http', meaning this is a full link and can be directly used for download
            link = raw_link
            try:
                download_file(link, pdf_name, dl_path)
                update_count += 1
            except:
                print('Cannot get file '+name+': '+link)
        else:
            if raw_link.startswith('/'):
                ## the link starts with a '/', like /document/xxx/xxx/2019-csr.pdf
                try:
                    ## first try with adding this part directly with root
                    link = root+raw_link
                    download_file(link, pdf_name, dl_path)
                    update_count += 1
                except:
                    ## if not do a search on the first trunk, like getting 'document' from the link and search in the original url
                    initial = raw_link.split('/')[1]
                    ## if we are able to find it in original link, paste and replace to get file
                    if url.find(initial)>=0:
                        link = url[:url.find(initial)] + raw_link
                    try:
                        download_file(link, pdf_name, dl_path)
                        update_count += 1
                    except:
                        print('Cannot get file '+name+': '+link)
            else:
                ## the link doesn't start with '/', like document/xxx/xxx/2019-csr.pdf, follow almost the same process
                try:
                    link = root+'/'+raw_link
                    download_file(link, pdf_name, dl_path)
                    update_count += 1
                except:
                    initial = raw_link.split('/')[0]
                    if url.find(initial)>=0:
                        link = url[:url.find(initial)] + raw_link
                    try:
                        download_file(link, pdf_name, dl_path)
                        update_count += 1
                    except:
                        print('Cannot get file '+name+': '+link)
    
    ## print to indicate the end of scraping for a firm
    print('Finished scraping for '+name+', '+str(update_count)+' files updated.')




def get_pillar3(scrapedPath,websitesPath,bank,filters,file_type='.pdf'):
    '''
    load website.xlsx, get all websites url for banks
    '''
    try:
        websites=pd.read_excel(websitesPath,index_col=0)
    except PermissionError:
        print('Please close Websites.xlsx file and execute this command cell again')
        return False
    
    if bank=='Allbanks':
        banks=list(websites.index)
    else :
        banks=[bank]
    
    for bank in banks:
        try:
            url=websites.loc[bank,'Website']
            if pd.isnull(url):
                print('No url detected, please check Row '+ bank+' of Websites.xlsx file')
                print('\n')
                continue
            else:
                if filters==False: #no keywords detected, just download all reports
                    get_all_file(bank,filters,url, scrapedPath, file_type)
                else: #keywords detected, download those who have all keywords in the report name
                    keywords_filter=get_filters(websitesPath,bank)
                    get_all_file(bank,keywords_filter,url, scrapedPath, file_type)

        except KeyError:
            print('No url detected, please check Row '+ bank+' of Websites.xlsx file')
            print('\n')
            continue
        print('\n')
    return
