"""
this code is created to fill
1-the separate gaps(single missing value)
2- gaps shorter than specified duration from nearest station if the nearest station has a gap at the same time
it will take the values from the second nearest station
the result will be saved in a database named 'database\\rainfall_fill1.db'
inputs:
        1- line 25 the rainfall database name
        2- line 28 the gaps database name
        3- line 62 the gaps database name
outputs:
        1-line 34 the resulted rainfall database

"""
#%%Liberaries
from IPython import get_ipython     # to reset the variable explorer each time
get_ipython().magic('reset -f')
import os                           # changing directory
os.chdir('C:\\Users\\Mostafa\\Desktop\\My Files\\thesis\\My Thesis\\Data_and_Models\\Data\\23Oct')
import sys
sys.path.append("C:\\Users\Mostafa\\Desktop\\My Files\\thesis\\My Thesis\\Data_and_Models\\Data\\23Oct\\functions")
import sqlite3                    # to create a database
#______________________________________________________________________________
# the database with gaps wanted to be filled 
db=sqlite3.connect('database\\fill5\\rainfall_fill5.db')
cursor=db.cursor()
# the database of the gaps created with gaps code
db1=sqlite3.connect('database\\fill5\\rainfall_fill5_gaps_v2.db')
cursor1=db1.cursor()
# the data base of the graph
#db2=sqlite3.connect('database\\fill3\\rainfall_fill3_fillsummer_graph.db')
#cursor2=db2.cursor()
# the resulting database after filling the wanted gaps
dbf=sqlite3.connect('database\\final\\rainfall_final.db')
cursorf=dbf.cursor()
#______________________________________________________________________________
#import numpy as np
#import matplotlib.pyplot as plt
#from dateformated import dateformated
from myround import myround
from termcolor import colored
#import pandas as pd
print('importing liberaries finished')
#%%
station=['Cojutepeque','Guadalupe','Ilobasco','Ilopango','Jerusalem',
     'La_Cima','Panchimalco','Puente_Viejo','San_Vicente','Tepezontes','Verapaz',
     'Zacatecoluca','Belloso','Hacienda_Melara','Melara','San_Vicente_Hidro',
     'Santiago_Nonualco','Tonacatepeque']
no=list(range(len(station)))
#import pickle 
#pickle.dump(station, open("save.p",'wb'))
#sss=pickle.load(open("save.p",'rb'))
#%% getting the list of nearest station
from nearest_station import nearest_station
nearest=nearest_station(station,1)
second_nearest=nearest_station(station,2)
third_nearst=nearest_station(station,3)
forth_nearest=nearest_station(station,4)
#%% gap duration
# to run it  you have to run the code gaps first as we here are using the resulting database 
from gap_duration import gap_duration
gaps=gap_duration(station,'database\\fill5\\rainfall_fill5_gaps_v2.db') 
#gaps1=gap_duration(station,'fill1\\rainfall_gaps_fill1.db')
#%% filling gaps less than 6 hours 
for j in range(len(station)):
#j=7
    #1- read the date and precipitation of the station
    cursor.execute("SELECT date FROM " + station[j]+ " ORDER BY date" )
    date_temp=cursor.fetchall()
    date_temp=[i[0] for i in date_temp] # conver the list of tuples to list of elements
    
    #date_temp=dateformated(date_temp)
    cursor.execute("SELECT precipitation FROM " + station[j]+ " ORDER BY date" )
    precipitation_temp=cursor.fetchall()
    precipitation_temp=[i[0] for i in precipitation_temp] # conver the list of tuples to list of elements
    
    print(str(j) +"-"+ station[j] + " :1-read the date and precipitation of the station")
    #_________________________________________________________________________________________________________________
    #2- replace the single empty values by average of previous and following values
#    for i in range(1,len(precipitation_temp)-1):
#        if precipitation_temp[i]== None and precipitation_temp[i-1]!= None and precipitation_temp[i+1]!= None:
#            precipitation_temp[i]=(float(precipitation_temp[i+1])+precipitation_temp[i-1])/2
#    
#    print(str(j) +"-"+ station[j]+ " :2-replace the single empty values by average of previous and following values")
    #_________________________________________________________________________________________________________________
    #3-create the database of the filled empty values
    cursorf.execute("CREATE TABLE " + station[j] + " ( id integer primary key,date TEXT ,precipitation REAL )")
    iid=[l+1 for l in range(len(precipitation_temp))] # create the id list
    
    h=zip(iid,date_temp,precipitation_temp)
    cursorf.executemany(" INSERT INTO " + station[j] + " ( id, date, precipitation) VALUES(?,?,?)",h)
    dbf.commit()
    
    print(str(j) +"-"+ station[j] + " :3-create the database of the filled single empty values")
    #_________________________________________________________________________________________________________________
    # bring the pricipitation and date of the nearest station
    #cursor.execute("SELECT date FROM " + nearest[j]+ " ORDER BY date" )
    #date_temp1=cursor.fetchall()
    ##date_temp=[i[0] for i in date_temp] # conver the list of tuples to list of elements
    #date_temp1=dateformated(date_temp1)
    #cursor.execute("SELECT precipitation FROM " + nearest[j]+ " ORDER BY date" )
    #precipitation_temp1=cursor.fetchall()
    #precipitation_temp1=[i[0] for i in precipitation_temp1] # conver the list of tuples to list of elements
    
    # 4-getting the gaps shorter than 6 hours
    short=[[],[]]
    #short[0] is the index inside gaps dictionary
    # short[1] is the gap duration
    for i in range(len(gaps[station[j]][2])):
        if len(gaps[station[j]][2][i]) > 8: # if the length of the date is more than 8 character so it is in terms of days "9 days, 1:30:00"
#              continue   # if it is in terms of days skip
            d=int(gaps[station[j]][2][i][:2])  # number of days
            if d <= 31:  # gaps shorter than 30 days
                short[0].append(i)  # index of the duration to use it to grap the start and end from gaps dictionary 
                short[1].append(d) # duration of the gap
            else:                # if the number of days is longer than 25 
                continue
        else:
            d=int(gaps[station[j]][2][i][-8:-6]) # duration less that a day
            if d <= 24 :  # 24 hours
                short[0].append(i)  # index of the duration to use it to grap the start and end from gaps dictionary 
                short[1].append(d) # duration of the gap
    
    print(str(j) +"-"+ station[j] + " :4-getting the gaps shorter than 6 hours")
    #_________________________________________________________________________________________________________________        
    #5- getting the start and end date of the gap from the gap dictionary
    s=[]; e=[]
    for i in short[0]:
        s.append(gaps[station[j]][0][i]) # get the starting date with the index from gaps dictionary
        e.append(gaps[station[j]][1][i]) # get the ending date with the index from gaps dictionary
    
    print(str(j) +"-"+ station[j] + " :5-getting the start and end date of the gap from the gap dictionary")
    #_________________________________________________________________________________________________________________
    #6- getting the precipitation values of the gap period from the nearest station
    date_temp1=[]
    precipitation_temp1=[]
    for i in range(len(s)):
        cursor.execute("SELECT date FROM " + nearest[j]+ " WHERE date >= '" + str(s[i]) +"' AND date <= '" + str(e[i]) + "' ORDER BY date" )
        d=cursor.fetchall()   # bring the dates of the gap
        date_temp1=date_temp1+d   # concatenate the dates into date_temp1 list
        cursor.execute("SELECT precipitation FROM " + nearest[j]+ " WHERE date >= '" + str(s[i]) +"' AND date <= '" + str(e[i]) + "' ORDER BY date" )
        p=cursor.fetchall()                         # bring the precipitation values
        precipitation_temp1=precipitation_temp1+p   # concatenate the precipitation values into precipitation_temp1
    
    precipitation_temp1=[i[0] for i in precipitation_temp1]
    date_temp1=[i[0] for i in date_temp1]
    
    print(str(j) +"-"+ station[j] + " :6-getting the precipitation values of the gap period from the nearest station")
    #_________________________________________________________________________________________________________________
    
    # check if the retrieved values of precipitation has a None values ?
    for i in range(len(date_temp1)):
        if precipitation_temp1[i]== None:  #precipitation_temp1[i]=None  date_temp1[0]='2012-03-05 13:15:00'
            # for the timesteps that in the form of YYYY-MM-DD HH-29-59 round the min and second to nearest 5
            if  int(date_temp1[i][-5:-3])%5 != 0 :# check the min if they could by divided by 5 if not round it to the nearest 5
                date_temp1[i]=date_temp1[i][:-5]+str(myround(int(date_temp1[i][-5:-3]),5))+':00'
            
            #1- try the nearest again if it gives empty so the timestep is different and by changing the min maybe there is a value
            cursor.execute("SELECT precipitation FROM " + nearest[j]+ " WHERE date = '" + date_temp1[i]+ "'" )
            pr=cursor.fetchall() 
            
            if pr==[]: # if empty so the the date does not exist 
                plus5=date_temp1[i][:14]+str(int(date_temp1[i][14:16])+5)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                # try date+5 min
                cursor.execute("SELECT precipitation FROM " + nearest[j]+ " WHERE date = '" + plus5+ "'" )
                pr=cursor.fetchall()
                if pr==[]: # if it gives an error and the date+5min also does not exist try date+10 min
                    plus10=date_temp1[i][:14]+str(int(date_temp1[i][14:16])+10)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                    cursor.execute("SELECT precipitation FROM " + nearest[j]+ " WHERE date = '" + plus10+ "'" )
                    pr=cursor.fetchall()
                    if pr==[]:
                        plus15=date_temp1[i][:14]+str(int(date_temp1[i][14:16])+15)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                        cursor.execute("SELECT precipitation FROM " + nearest[j]+ " WHERE date = '" + plus15+ "'" )
                        pr=cursor.fetchall()
                        if pr==[]:
                            minus5=date_temp1[i][:14]+str(int(date_temp1[i][14:16])-5)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                            cursor.execute("SELECT precipitation FROM " + nearest[j]+ " WHERE date = '" + minus5+ "'" )
                            pr=cursor.fetchall()
                            if pr==[]: #if still empty so it is not at all at this table so put it None to search for it in another table
                                pr=[(None,)]
                                precipitation_temp1[i]=pr[0][0]
        #                        print(date_temp1[i]+" doesn't exist in nearest station")
                            else:
                                precipitation_temp1[i]=pr[0][0]
                                print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+" filled from nearest station minus5 but maybe none value ")
                        else: 
                            precipitation_temp1[i]=pr[0][0]
                            print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+" filled from nearest station plus15 but maybe none value ")
                    else:
                        precipitation_temp1[i]=pr[0][0]
                        print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+" filled from nearest station plus10 but maybe none value ")
                else:    # != []:# if the retrieved value is number or None 
                    precipitation_temp1[i]=pr[0][0]
                    print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+" filled from nearest station plus5 but maybe none value ")
            else: # if the retrieved value is none 
                precipitation_temp1[i]=pr[0][0]
                print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+" exist with the same time step at the nearst station but None so go to second nearest ")
            
            
            # the stored value from the previous line may be still None
            if precipitation_temp1[i]!=None:
                print colored(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+" filled from nearest station",'magenta' )
                continue
            
            # so the neither the date nor the date+5min or the date+10 min exists in the nearest table or they give a none value
    #        if pr[0][0] == None:
            cursor.execute("SELECT precipitation FROM " + second_nearest[j]+ " WHERE date = '" + date_temp1[i]+ "'" )
            pr=cursor.fetchall()
            if pr==[]:
                plus5=date_temp1[i][:14]+str(int(date_temp1[i][14:16])+5)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                # try date+5 min
                cursor.execute("SELECT precipitation FROM " + second_nearest[j]+ " WHERE date = '" + plus5+ "'" )
                pr=cursor.fetchall()
                if pr==[]: # if it gives an error and the date+5min also does not exist try date+10 min
                    plus10=date_temp1[i][:14]+str(int(date_temp1[i][14:16])+10)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                    cursor.execute("SELECT precipitation FROM " + second_nearest[j]+ " WHERE date = '" + plus10+ "'" )
                    pr=cursor.fetchall()
                    if pr==[]:
                        plus15=date_temp1[i][:14]+str(int(date_temp1[i][14:16])+15)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                        cursor.execute("SELECT precipitation FROM " + second_nearest[j]+ " WHERE date = '" + plus15+ "'" )
                        pr=cursor.fetchall()
                        if pr==[]:
                            minus5=date_temp1[i][:14]+str(int(date_temp1[i][14:16])-5)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                            cursor.execute("SELECT precipitation FROM " + second_nearest[j]+ " WHERE date = '" + minus5+ "'" )
                            pr=cursor.fetchall()
                            if pr==[]: #if still empty so it is not at all at this table so put it None to search for it in another table
                                pr=[(None,)]
                                precipitation_temp1[i]=pr[0][0]
        #                        print(date_temp1[i]+" doesn't exist in nearest station")
                            else:
                                precipitation_temp1[i]=pr[0][0]
                                print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+" filled from second nearest station minus5 but maybe none value ")
                        else: 
                            precipitation_temp1[i]=pr[0][0]
                            print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+" filled from second nearest station plus15 but maybe none value ")
                    else:
                        precipitation_temp1[i]=pr[0][0]
                        print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+"filled from second nearest station plus10 but maybe none value ")
                else:
                    precipitation_temp1[i]=pr[0][0]
                    print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+"filled from second nearest station plus5 but maybe none value ")
            else:
                precipitation_temp1[i]=pr[0][0]
                print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+"exist with the same time step at the second nearst station but  maybe None value")
            
            
             # the stored value from the previous line may be still None
            if precipitation_temp1[i]!=None:
                print colored(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+" filled from second nearest station",'magenta' )
                continue
            
             # so the neither the date nor the date+5min or the date+10 min exists in the nearest table or they give a none value
    #        if pr[0][0] == None:
            cursor.execute("SELECT precipitation FROM " + third_nearst[j]+ " WHERE date = '" + date_temp1[i]+ "'" )
            pr=cursor.fetchall()
            if pr==[]:
                plus5=date_temp1[i][:14]+str(int(date_temp1[i][14:16])+5)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                # try date+5 min
                cursor.execute("SELECT precipitation FROM " + third_nearst[j]+ " WHERE date = '" + plus5+ "'" )
                pr=cursor.fetchall()
                if pr==[]: # if it gives an error and the date+5min also does not exist try date+10 min
                    plus10=date_temp1[i][:14]+str(int(date_temp1[i][14:16])+10)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                    cursor.execute("SELECT precipitation FROM " + third_nearst[j]+ " WHERE date = '" + plus10+ "'" )
                    pr=cursor.fetchall()
                    if pr==[]:
                        plus15=date_temp1[i][:14]+str(int(date_temp1[i][14:16])+15)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                        cursor.execute("SELECT precipitation FROM " + third_nearst[j]+ " WHERE date = '" + plus15+ "'" )
                        pr=cursor.fetchall()
                        if pr==[]:
                            minus5=date_temp1[i][:14]+str(int(date_temp1[i][14:16])-5)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                            cursor.execute("SELECT precipitation FROM " + third_nearst[j]+ " WHERE date = '" + minus5+ "'" )
                            pr=cursor.fetchall()
                            if pr==[]: #if still empty so it is not at all at this table so put it None to search for it in another table
                                pr=[(None,)]
                                precipitation_temp1[i]=pr[0][0]
        #                        print(date_temp1[i]+" doesn't exist in nearest station")
                            else:
                                precipitation_temp1[i]=pr[0][0]
                                print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+" filled from third nearest station minus5 but maybe none value ")
                        else: 
                            precipitation_temp1[i]=pr[0][0]
                            print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+" filled from third nearest station plus15 but maybe none value ")
                    else:
                        precipitation_temp1[i]=pr[0][0]
                        print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+"filled from third nearest station plus10 but  maybe None value")
                else:
                    precipitation_temp1[i]=pr[0][0]
                    print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+"filled from third nearest station plus5 but  maybe None value")
            else:
                precipitation_temp1[i]=pr[0][0]
                print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+"exist at the third nearst station but  maybe None value")
            
             # the stored value from the previous line may be still None
            if precipitation_temp1[i]!=None:
                print colored(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+" filled from third nearest station",'magenta' )
                continue
            
            
             # so the neither the date nor the date+5min or the date+10 min exists in the nearest table or they give a none value
    #        if pr[0][0] == None:
            cursor.execute("SELECT precipitation FROM " + forth_nearest[j]+ " WHERE date = '" + date_temp1[i]+ "'" )
            pr=cursor.fetchall()
            if pr==[]:
                plus5=date_temp1[i][:14]+str(int(date_temp1[i][14:16])+5)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                # try date+5 min
                cursor.execute("SELECT precipitation FROM " + forth_nearest[j]+ " WHERE date = '" + plus5+ "'" )
                pr=cursor.fetchall()
                if pr==[]: # if it gives an error and the date+5min also does not exist try date+10 min
                    plus10=date_temp1[i][:14]+str(int(date_temp1[i][14:16])+10)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                    cursor.execute("SELECT precipitation FROM " + forth_nearest[j]+ " WHERE date = '" + plus10+ "'" )
                    pr=cursor.fetchall()
                    if pr==[]:
                        plus15=date_temp1[i][:14]+str(int(date_temp1[i][14:16])+15)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                        cursor.execute("SELECT precipitation FROM " + forth_nearest[j]+ " WHERE date = '" + plus15+ "'" )
                        pr=cursor.fetchall()
                        if pr==[]:
                            minus5=date_temp1[i][:14]+str(int(date_temp1[i][14:16])-5)+date_temp1[i][16:] # increase the minutes by 5 min maybe different time steps is the reason of the error
                            cursor.execute("SELECT precipitation FROM " + forth_nearest[j]+ " WHERE date = '" + minus5+ "'" )
                            pr=cursor.fetchall()
                            if pr==[]: #if still empty so it is not at all at this table so put it None to search for it in another table
                                pr=[(None,)]
                                precipitation_temp1[i]=pr[0][0]
        #                        print(date_temp1[i]+" doesn't exist in nearest station")
                            else:
                                precipitation_temp1[i]=pr[0][0]
                                print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+" filled from forth nearest station minus5 but maybe none value ")
                        else: 
                            precipitation_temp1[i]=pr[0][0]
                            print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+" filled from forth nearest station plus15 but maybe none value ")
                    else:
                        precipitation_temp1[i]=pr[0][0]
                        print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+"filled from forth nearest station plus10 but  maybe None value")
                else:
                    precipitation_temp1[i]=pr[0][0]
                    print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+"filled from forth nearest station plus5 but  maybe None value")
            else:
                precipitation_temp1[i]=pr[0][0]
                print(str(j+1)+'-'+station[j]+'-'+date_temp1[i]+"exist at the forth nearst station but  maybe None value")           
            
            
            if precipitation_temp1[i]==None:
                print colored("please add the fifth nearest station tables",'red')
            
    # 7- update the precipitation values in the table at specific dates
    for i in range(len(date_temp1)):
        #i=0
        dbf.execute("UPDATE " + station[j] + " SET precipitation = ? WHERE date = ? ", (precipitation_temp1[i], date_temp1[i]) )
dbf.commit()

print(str(j) +"-"+ station[j] + " :7-update the precipitation values in the table at specific dates")
#_________________________________________________________________________________________________________________
del d,date_temp,date_temp1,precipitation_temp, precipitation_temp1,p,j,i,h, plus10,plus15,plus5, minus5
#%%plot
#j=0
#cursorf.execute("select date from "+ station[j] )
#date=cursorf.fetchall()
#date=dateformated(date)
#cursorf.execute("select precipitation from "+ station[j] )
#precipitation=cursorf.fetchall()
#precipitation=[i[0] for i in precipitation]
#precipitation.count(None)
#cursor.execute("select precipitation from " +station[j])
#precipitation_before=cursor.fetchall()
#precipitation_before=[i[0] for i in precipitation_before]
#precipitation_before.count(None)
#
#from gap_duration import gap_duration
#gaps=gap_duration([station[j]])
#%%
#plt.figure(j+1,figsize=(15,8))
##date=dates.date2num(x)
#plt.plot(date,precipitation)
##plt.plot(date_temp1,precipitation_temp1) 
#plt.xlabel('Date')
#plt.ylabel('Precipitation mm')
#plt.title('Rainfal Station: '+str(station[j]))
#plt.show()
#plt.legend(station[j], nearest[j])
