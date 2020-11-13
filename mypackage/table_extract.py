# -*- coding: utf-8 -*-
"""
This package contains all funtions needed 
to extract a particular table from multiple excel reports
@author: Ruoyi Ma
GBI - August 2020
"""
import pandas as pd
import numpy as np
import os
import shutil


def get_keywords(keywordsPath,bank,table):
    '''
    load keywords from an excel file that will be used for table extraction
    '''
    
    try:
        keywords=pd.read_excel(keywordsPath,index_col=0)
    except PermissionError:
        print('Please close Table_Keywords.xlsx file and execute this command cell again')
        return False
        
    while True:
        print(bank+' - '+table+ ' Table\n')
        try:
            keywordsList=keywords.loc[bank,table]
            
            if pd.isnull(keywordsList):
                print('No keywords detected, please check Row '+ bank+' and Column ' + table + ' of Table_Keywords.xlsx file')
                print('\n')
                return False
            else:
                keywordsList= [s.strip() for s in keywordsList.split(',')]
                print('Keywords: '+str(keywordsList))
                return (keywordsList)
            
        except KeyError:
            print('No keywords detected, please check Row '+ bank+' and Column ' + table + ' of Table_Keywords.xlsx file')
            print('\n')
            return False



def strInTable(list_str2Match,listOfTables,minMatch=False):
    
    '''
    given a list of keywords and a list a tables, 
    return all tables that contain all keywords by default(minMatch=False)
    
    if 5 keywords are provided and we set minMatch=3
    return all tables that contain at least 3 of the 5 keywords
    
    '''
    if minMatch==False:
        minMatch=len(list_str2Match) #Default case, look for table that match all strings
    if minMatch>len(list_str2Match):
        print('minMatch has to be smaller than '+str(len(list_str2Match))+',we will look for tables that contain all given keywords')
        minMatch=len(list_str2Match)
    
    l=[]
    for table in listOfTables:
        table_i=table.reset_index().astype(str)
        Col=table_i.columns
        
        countMatch=0
        for string in list_str2Match:
            matched_str=0
            for c in Col:
                matched_str=0
                match=table_i[(table_i[c].str.contains(string,na=False,case=False))]
                if match.empty==False:  #if matched this string at a col, break the for col loop
                    matched_str=1
                    break
            countMatch+=matched_str
        
        if countMatch>=minMatch: #if matched at least minMatch string in list_str2Match
            l=l+[table_i]
    
    print('--> Found '+str(len(l))+' table(s) which contain at least ' + str(minMatch)+ ' keyword(s)')
    return(l)
        

def dropNaN(listOfTables):
    '''
    delete empty roll&columns of all tables in the list of tables
    '''
    for table in listOfTables:
        table.replace('nan', np.NaN,inplace=True)
        table.dropna(axis=0,how='all',inplace=True)
        table.dropna(axis=1,how='all',inplace=True)
    return(listOfTables)


def get_Table(path,list_str2Match,minMatch=False):
    '''
    give a .xlsx/.xls/.csv file an a list of keywords, return a list of tables 
    in this file which contain all keywords(by default)
    
    minMatch: minimum number of keywords that the table should contain
    '''
    #Default match all strings
    
    files=os.listdir(path)
    df_list=[]
    file_count=0
    for file in files:
        listOfTables=[]
        if file.endswith(('.xlsx','xls','csv')):
            file_count+=1
            print(file+' started')
            tables=pd.read_excel(path+"\\"+file,index_col=0,header=None,sheet_name=None)
            listOfTables=list(tables.values())

            matchedTables=strInTable(list_str2Match,listOfTables,minMatch)
            matchedTables=dropNaN(matchedTables)
            df_list=df_list+matchedTables
    if len(df_list)==0:
        if  file_count==0:
            print('No csv/xlsx/xls files detected in this folder')
        else:
            print('No table detected, please check the original csv/pdf files or refine the keywords')
    return (df_list)



def export_df_list(df_list,path,filename):
    
    '''
    Export all tables to a given folder under a given name
    '''
    if not os.path.exists(path):
        os.makedirs(path)
    
    with pd.ExcelWriter(path+filename) as writer:  
        i=0
        for table in df_list:
            table.to_excel(writer, sheet_name=str(i),index=None,header=None)
            i+=1  
    return




def extract_tableFcsv(bank,table,reportPath,keywordsPath,readfolder,savefolder,minMatch=False):

    '''
    Take csv/xlsx/xls reports from readfolder(default 'csv reports'), extract tables using keywords in Table_keywords.xlsx
    Save extracted tables in 1 csv under the savefolder (defult 'selected tables')
    
    ---
    bank='Allbanks', table extraction for all banks
    bank='Toronto Dominion', table extraction only for this particular bank

    '''
    if readfolder=='csv reports update': 
        print('Once extrction done, do you want to move the csv files in /csv reports update to /csv reports?\
               \n Enter (Y/N)')
        move_files=input()
    
    if bank=='Allbanks' :
        banks = [o for o in os.listdir(reportPath) if os.path.isdir(os.path.join(reportPath,o))]

    else:
        banks=[bank]

    for bank in banks:
        readpath=(reportPath+'\\'+bank+'\\'+readfolder+'\\')
        savepath=(reportPath+'\\'+bank+'\\'+savefolder+'\\')
        saveas=bank+'-'+table+'.xlsx'
        keywordsList=get_keywords(keywordsPath,bank,table)
        df_list=[]
        if keywordsList==False:
            continue
        
        if readfolder=='csv reports':
            try :
                new_tables=get_Table(readpath,keywordsList,minMatch)
                df_list=new_tables
            except PermissionError:
                print('Please close all opened csv files and launch this command cell again')
                return
            except FileNotFoundError:
                print('No files detected, please check if bank and folder names are entered correctly')
                return
            
        if readfolder=='csv reports update': 
            if os.path.exists(savepath+saveas):
                try:
                    old_tables=pd.read_excel(savepath+saveas,header=None,sheet_name=None)
                except PermissionError:
                    print('please close all opened excel files')
                    return
                old_tables=list(old_tables.values())
                new_tables=get_Table(readpath,keywordsList,minMatch)
                df_list=old_tables+new_tables
            else:
                df_list=get_Table(readpath,keywordsList,minMatch)
            
            if move_files=='Y': #move all files from csv reports update to csv reports
                for file in os.listdir(readpath):
                    shutil.move(readpath+file,reportPath+'\\'+bank+'\\csv reports\\'+file) 


        print('\n')
        if len(df_list)>0: #won't export file if no table detected
            export_df_list(df_list,savepath,saveas)
    print('Extraction finished')
    return



def tableFcsv(reportPath,keywordsPath):
    
    '''
    Create the user interface on Jupyter notebook
    Ask user to input parameters
    '''
    print("Do you want to create a new type of table or update the existing table? \
        \nPlease enter 'C' for create or 'U' for update")
    Action=input()
    Action=Action.lower()
    print("\nWhat is the name of that table?")
    table=input()
    print("\nFor 1 bank or for all banks? Please enter name of the bank(e.g.'Barclays') or 'Allbanks'")
    bank=input()
    print('\n')
    
    savefolder='selected tables'
    if Action=='u':
        readfolder='csv reports update'
        try:
            extract_tableFcsv(bank,table,reportPath,keywordsPath,readfolder,savefolder)
        except FileNotFoundError:
            print("Can't find the bank folder, please review the bank name you entered")

    elif Action=='c':
        readfolder='csv reports'
        try:
            extract_tableFcsv(bank,table,reportPath,keywordsPath,readfolder,savefolder)
        except FileNotFoundError:
            print("Can't find the bank folder, please review the bank name you entered")
    else:
        print("The input action is not valide, please enter 'C' for create or 'U' for update")
    print('\n')
    return


