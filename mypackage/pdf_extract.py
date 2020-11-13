# -*- coding: utf-8 -*-
"""
This package contains all funtions needed 
to extract tables from a pdf report and convert it to an excel report
@author: Ruoyi Ma
GBI - August 2020
"""

import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

import camelot
import pandas as pd
import regex as re
import os
import shutil





def get_tableFpdf(file,startpage=1,endpage='end',flag_size=True,row_tol=False):
    '''
    Input a pdf file, output a list of tables scraped from the pdf
    
    scrape table page by page (don't use the integrated function of camelot because
    it can't skip empty page and return Error)

    Default settings: 
    startpage='1'
    endpage='end'
    flag_size=True -- detect superscript and put it into <s>superscript</s> format
    rol_tol=False -- (int, optional (default: 2)) – Tolerance parameter used to combine text vertically, to generate rows.
    '''
    
    table_list=list()

    if endpage=='end':
        page=startpage
        while True:
            try:
                tables=camelot.read_pdf(file,pages=str(page),flagsize=True,flavor='stream')
                table_list.append(tables)
                page+=1
            
            except ValueError:
                print('page '+str(page)+' is empty')
                page+=1
 
            except IndexError:
                break
            
            except NotImplementedError:
                print('check this file later, probably secured/encrypted')
                break
            except PermissionError:
                break
            
    elif startpage>endpage:
        print('endpage have to be >= start page')       
    
    else:
        page=startpage
        while page<=endpage:
            try:
                tables=camelot.read_pdf(file,pages=str(page),flagsize=True,flavor='stream') 
                #flagsize=True, detect superscript, save as <s>superscript</s>
                table_list.append(tables)
                page +=1
                
            except ValueError:
                print('page '+str(page)+' is empty')
            except IndexError:
                break
            
            except NotImplementedError:
                print('check this file later, probably secured/encrypted')
                break
            except PermissionError:
                break

    return(table_list)


def clean_tableFpdf(df):
    '''
    Basic table cleaning. Input a df, output a df
    '''
    df.replace(regex=r'\n', value='',inplace=True)
    df.replace(regex=r'\n ', value='',inplace=True)
    df.replace(regex=r'[ ]{2,}', value=' ',inplace=True)
    df.replace(regex=r'–', value=' ',inplace=True) #To avoid error when read by excel
    df.replace(regex=r'-', value=' ',inplace=True)
    df.replace(regex=r'<s>',value='[',inplace=True) #put all superscripts into bracket
    df.replace(regex=r'</s>',value=']',inplace=True)
    df.replace(regex=r' ]',value=']',inplace=True)
    return (df)


def get_num_ratio(table): 
    '''
    Get the numerical ratio of a given table. Input a df, output a numerical value
    '''
    all_values=table.values
    l=list()
    isblank=0
    numerical_cells=0
    for row in all_values:
        for cell in row:
            matched = re.match('^\(*[0-9., ]+\)*$', cell)
            blank = re.search('^\s*$', cell)
            if matched!=None:
                numerical_cells+=1
                l.append(matched)
            if blank!=None:
                isblank+=1
    nb_cells=table.size-isblank
    ratio=numerical_cells/nb_cells

    if nb_cells<=2:
        ratio=0
    return (ratio)

def export_tableFpdf(tables,savepath,ratiothreshold):
    '''
    export all numerical tables(numerical ratio>ratio threshold) scraped from the pdf file
    output a csv file which contain multiple sheets, called 'page number-table number'
    e.g. sheet '31-2' contains the second table scrapped from page 31 of the pdf report
    
    '''
    writer = pd.ExcelWriter(savepath, engine='xlsxwriter')
    for page in tables:
        for table in page:
            table_cleaned=clean_tableFpdf(table.df)
            num_ratio=get_num_ratio(table_cleaned)
            if num_ratio>=ratiothreshold: #parameter to be adjusted
                parsingReport=table.parsing_report
                page=parsingReport['page']
                order=parsingReport['order']
                table_cleaned.to_excel(writer,sheet_name=str(page)+'-'+str(order),header=None,index=None)
                
    writer.save()
    return



def extract_tableFpdf(bank,reportPath,readfolder,savefolder,ratiothreshold=0.2):
    '''
    Main function
    scrape and export tables for a given bank or for all banks in the reportPath folder
    read pdf from readfolder
    save csv to savefolder
    default numerical ratiothreshold for table filtering is 0.2
    '''

    if bank=='Allbanks' :
        banks = [o for o in os.listdir(reportPath) if os.path.isdir(os.path.join(reportPath,o))]

    else:
        banks=[bank]

    for bank in banks:
        readpath=(reportPath+'\\'+bank+'\\'+readfolder+'\\')
        savepath=(reportPath+'\\'+bank+'\\'+savefolder+'\\')
        if not os.path.exists(savepath):
            raise FileNotFoundError('format not correct')


        files=os.listdir(readpath)
        if len(files)==0:
            print('No pdf detected in folder '+ readfolder)
        
        pdfcount=0
        for file in files:
            if file.endswith('.pdf'):
                filepath=readpath+'\\'+file
                pdfcount+=1
                print(bank+' '+file[:-4]+' extraction started')
                tables=get_tableFpdf(filepath)
                export_tableFpdf(tables,savepath+bank+'-'+file[:-4]+'.xlsx',ratiothreshold)
                print(bank+' '+file[:-4]+' extraction completed\n')
                
                if readfolder=='pdf reports update':
                    shutil.move(filepath,reportPath+'\\'+bank+'\\'+'pdf reports\\'+file)
                
        print(bank+' extraction completed, converted '+str(pdfcount)+' pdf to csv\n\n')

    print('Extraction finished')                
    
    return

def tableFpdf(reportPath):
    
    '''
    Create the user interface on Jupyter notebook
    Ask user to input parameters
    '''
    print("Do you want to recreate CSV reports or only update the latest periods? \
        \nPlease enter 'C' for create or 'U' for update")
    Action=input()  
    Action=Action.lower()
    print("\nFor 1 bank or for all banks? Please enter name of the bank(e.g.'Barclays') or 'Allbanks'")
    bank=input()
    print('\n')
    if Action=='u':
        readfolder='pdf reports update'
        savefolder='csv reports update'
        try:
            extract_tableFpdf(bank,reportPath,readfolder,savefolder)
        except FileNotFoundError:
            print("Can't find the bank folder, please review the bank name you entered")


    elif Action=='c':
        readfolder='pdf reports'
        savefolder='csv reports'
        try:
            extract_tableFpdf(bank,reportPath,readfolder,savefolder)
        except FileNotFoundError:
            print("Can't find the bank folder, please review the bank name you entered")
    else:
        print("The input action is not valide, please enter 'C' for create or 'U' for update")
    print('\n')
    return