# -*- coding: utf-8 -*-
"""
This package contains functions to combine OV1 tables from different 
reporting periods for the following banks:
    Bank of Montreal
    Bank of Nova Scotia
    National Bank of Canada
    Toronto Dominion
    Royal Bank of Canada

@author: Ruoyi Ma
GBI - August 2020
"""
import pandas as pd
import numpy as np



def findIdx(df, pattern):
    '''
    Get the index(row and col) of a matched cell    
    '''
    return df.apply(lambda x: x.str.lower().str.contains(pattern.lower(),na=False,regex=True)).values.nonzero()



def dropNaN(listOfTables):
    '''
    For all tables in the list of tables, delete empty row&columns
    '''
    for table in listOfTables:
        table.replace('nan', np.NaN,inplace=True)
        table.dropna(axis=0,how='all',inplace=True)
        table.dropna(axis=1,how='all',inplace=True)
    return(listOfTables)


'''
Below are the functions to merge the "OV1" tables from different time periods.
There are 1 particular function for each bank
These functions are similar but are still different from one another
due to the different reporting structure of each bank
'''
def update_BMO(previous_df,new_df_list):
    merged_df=previous_df
    for df in new_df_list:
        #Delete all rows below the 'Total'
        (row_bottomLeftCell,col_bottomLeftCell)=findIdx(df, r'^Total$')
        df=df.iloc[:row_bottomLeftCell[0]+1]

        #Clean the table
        df=dropNaN([df])[0] 
        df=df.iloc[:,:-1] #drop the last col because it's max capital requirement

        #Match date columns with different formats
        (row_date,col_date)=findIdx(df, r'[Q][0-4]?[\s\/](20)\d{2}$')

        colDate=[]
        for i in range(len(row_date)):
            date=df.iloc[row_date[i],col_date[i]]
            date=date[3:]+' '+date[:2]#Change from format 2020 Q2 to Q2 2020, so that can be ordered in alphabetical order 
            colDate.append(date) #Store the date

        df.columns=['nb','Entity']+colDate #Rename the columns

        #Delete all columns before the 'nb' column and above the Credit risk row
        (row_topLeftCell,col_topLeftCell)=findIdx(df,r'Credit risk')
        df=df.iloc[row_topLeftCell[0]:,col_topLeftCell[0]-1:] #-1 so that we have the number column

        df=df.set_index('nb',drop=True)
        pd.options.display.float_format = '{:,.0f}'.format

        if merged_df.empty==True:
            merged_df=df
        else:
            merged_df = df.join(merged_df.drop(merged_df.columns.intersection(df.columns), axis=1))  
            df=df.drop(columns=['Entity'])
            res=df.reindex(columns=merged_df.columns.union(df.columns))
            res.update(merged_df)
    res=res[res.columns[::-1]]
    return(res)


def update_BNS(previous_df,new_df_list):
    merged_df=previous_df
    for df in new_df_list:
        #Delete all rows below the 'Total'
        (row_bottomLeftCell,col_bottomLeftCell)=findIdx(df, r'^Total')
        df=df.iloc[:row_bottomLeftCell[0]+1]

        #Clean the table
        df=dropNaN([df])[0] 
        df=df.iloc[:,:-1] #drop the last col because it's max capital requirement

        #Match date columns with different formats
        (row_date,col_date)=findIdx(df, r'[Q][0-4]?\s(20)\d{2}$')
        colDate=[]
        for i in range(len(row_date)):
            date=df.iloc[row_date[i],col_date[i]]
            date=date[3:]+' '+date[:2]#Change from format 2020 Q2 to Q2 2020, so that can be ordered in alphabetical order 
            colDate.append(date) #Store the date

        df.columns=['nb','Entity']+colDate #Rename the columns

        #Delete all columns before the 'nb' column and above the Credit risk row
        (row_topLeftCell,col_topLeftCell)=findIdx(df,r'Credit risk')
        df=df.iloc[row_topLeftCell[0]:,col_topLeftCell[0]-1:] #-1 so that we have the number column

        df=df.set_index('nb',drop=True)
        pd.options.display.float_format = '{:,.0f}'.format

        if merged_df.empty==True:
            merged_df=df
        else:
            merged_df = df.join(merged_df.drop(merged_df.columns.intersection(df.columns), axis=1))  

            df=df.drop(columns=['Entity'])
            res=df.reindex(columns=merged_df.columns.union(df.columns))
            res.update(merged_df)
    res=res[res.columns[::-1]]
    return(res)



def update_NBC(previous_df,new_df_list):
    merged_df=previous_df
    for df in new_df_list:
        #Delete all rows below the 'Total'
        (row_bottomLeftCell,col_bottomLeftCell)=findIdx(df, r'^Total')
        df=df.iloc[:row_bottomLeftCell[0]+1]

        #Clean the table
        df=dropNaN([df])[0] 
        df=df.iloc[:,:-1] #drop the last col because it's max capital requirement, should be changed for other tables

        #Match date columns with different formats
        (row_date,col_date)=findIdx(df, r'[Q][0-4]?\s(20)\d{2}$')
        colDate=[]
        for i in range(len(row_date)):
            date=df.iloc[row_date[i],col_date[i]]
            date=date[3:]+' '+date[:2]#Change from format 2020 Q2 to Q2 2020, so that can be ordered in alphabetical order 
            colDate.append(date) #Store the date

        df.columns=['nb','Entity']+colDate #Rename the columns

        #Delete all columns before the 'nb' column and above the Credit risk row
        (row_topLeftCell,col_topLeftCell)=findIdx(df,r'Credit risk')
        df=df.iloc[row_topLeftCell[0]:,col_topLeftCell[0]-1:] #-1 so that we have the number column

        df=df.set_index('nb',drop=True)
        pd.options.display.float_format = '{:,.0f}'.format

        if merged_df.empty==True:
            merged_df=df
        else:
            merged_df = df.join(merged_df.drop(merged_df.columns.intersection(df.columns), axis=1))  

            df=df.drop(columns=['Entity'])
            res=df.reindex(columns=merged_df.columns.union(df.columns))
            res.update(merged_df)
    res=res[res.columns[::-1]]
    return(res)


def update_TD(previous_df,new_df_list):

    merged_df=previous_df
    for df in new_df_list:
        #Specific for canadian banks, to get rid of merged cells
        df=df.fillna(method='ffill',axis=1)
        
        #Delete all rows below the 'Total'
        (row_bottomLeftCell,col_bottomLeftCell)=findIdx(df, r'^Total')
        df=df.iloc[:row_bottomLeftCell[0]+1]

        #Clean the table
        df=dropNaN([df])[0] 

        #Drop the Minimum Capital Requirements columns
        (row_capReq,col_capReq)=findIdx(df, r'minimum capital')
        df.drop(df.columns[col_capReq], axis = 1,inplace=True)

        (row_empty,col_empty)=findIdx(df, r'^[/$]$')
        df.drop(df.columns[col_empty], axis = 1,inplace=True)

        #Match date columns with different formats
        (row_date,col_date)=findIdx(df, r'[Q][0-4]$')


        colDate=[]
        for i in range(len(row_date)):
            date=str(df.iloc[row_date[i]-1,col_date[i]]) + ' ' + str(df.iloc[row_date[i],col_date[i]])

            colDate.append(date) #Store the date


        (row_topLeftCell,col_topLeftCell)=findIdx(df,r'Credit risk')
         #Delete all columns before the 'nb' column
        df=df.iloc[:,col_topLeftCell[0]+2:]

        df.columns=['Entity','nb']+colDate #Rename the columns

        #Delete all rows above the Credit risk row
        df=df.iloc[row_topLeftCell[0]:,:]

        df=df.set_index('nb',drop=True)
        pd.options.display.float_format = '{:,.0f}'.format

        if merged_df.empty==True:
            merged_df=df
        else:
            merged_df = df.join(merged_df.drop(merged_df.columns.intersection(df.columns), axis=1))  

            df=df.drop(columns=['Entity'])
            res=df.reindex(columns=merged_df.columns.union(df.columns))
            res.update(merged_df)
    res=res[res.columns[::-1]]
    return(res)



def update_RBC(previous_df,new_df_list):
    merged_df=previous_df
    for df in new_df_list:

        #Delete all rows below the 'Total'
        (row_bottomLeftCell,col_bottomLeftCell)=findIdx(df, r'^Total')
        df=df.iloc[:row_bottomLeftCell[0]+1]

        #Clean the table
        df=dropNaN([df])[0] 

        #Drop the Minimum Capital Requirements columns
        (row_capReq,col_capReq)=findIdx(df, r'minimum|minimum capital')
        df.drop(df.columns[col_capReq], axis = 1,inplace=True)

        #Drop the change column
        (row_change,col_change)=findIdx(df, r'^Change$')
        df.drop(df.columns[col_change], axis = 1,inplace=True)

        #(row_empty,col_empty)=findIdx(df, r'^[/$]$')
        #df.drop(df.columns[col_empty], axis = 1,inplace=True)

        #Match date columns with different formats
        #(row_date,col_date)=findIdx(df, r'[0-9]+[.][0-9]+[.](20)\d{2}$')
        #(row_date,col_date)=findIdx(df, r'[Q][0-4][\/](20|19|18)$')
        #(row_date,col_date)=findIdx(df, r'[Q][0-4]$')
        (row_date,col_date)=findIdx(df, r'October|July|January|April')

        colDate=[]
        for i in range(len(row_date)):
            date=str(df.iloc[row_date[i]+1,col_date[i]]) + ' ' + str(df.iloc[row_date[i],col_date[i]])

            colDate.append(date) #Store the date


        (row_topLeftCell,col_topLeftCell)=findIdx(df,r'Credit risk')
         #Delete all columns before the 'nb' column
        df=df.iloc[:,col_topLeftCell[0]-1:]

        df.columns=['nb','Entity']+colDate #Rename the columns

        #Delete all rows above the Credit risk row
        df=df.iloc[row_topLeftCell[0]:,:]

        df=df.set_index('nb',drop=True)
        pd.options.display.float_format = '{:,.0f}'.format

        if merged_df.empty==True:
            merged_df=df

        else:
            merged_df = df.join(merged_df.drop(merged_df.columns.intersection(df.columns), axis=1))  

            df=df.drop(columns=['Entity'])
            res=df.reindex(columns=merged_df.columns.union(df.columns))
            res.update(merged_df)
    res=res[res.columns[::-1]]
    return(res)



def table_combine(bank,table,reportPath):
    '''
    Create the user interface on Jupyter notebook
    Ask user to input parameters
    '''
    
    new_df_list=pd.read_excel(reportPath+'\\'+bank+'\\selected tables\\'+bank+'-'+table+'.xlsx',header=None,sheet_name=None)
    new_df_list=list(new_df_list.values())
    
    previous_df=pd.DataFrame()
    
    if bank=='Bank of Montreal':
        combined=update_BMO(previous_df,new_df_list)
        
    elif bank=='Bank of Nova Scotia':
        combined=update_BNS(previous_df,new_df_list)
        
    elif bank=='National Bank of Canada':
        combined=update_NBC(previous_df,new_df_list)
    
    elif bank=='Toronto Dominion':
        combined=update_TD(previous_df,new_df_list)
    
    elif bank=='Royal Bank of Canada':
        combined=update_RBC(previous_df,new_df_list)     
        
    else:
        print('No existing function to combine the bank you want, Please do it manually :)')
        return
    
    combined.to_excel(reportPath+'\\'+bank+'\\combined tables\\'+bank+'-'+table+'.xlsx') 
    return(combined)
    
    
    
    