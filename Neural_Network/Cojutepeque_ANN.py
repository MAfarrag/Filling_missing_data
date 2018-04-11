
"""
"""
#%%Liberaries
from IPython import get_ipython   # to reset the variable explorer each time
get_ipython().magic('reset -f')
import numpy as np  # for importing the files & arithmaic operations
import pandas as pd
from sklearn import preprocessing  # for the standarization of data
from sklearn.neural_network import MLPRegressor # the MLP neural network
from sklearn.metrics import explained_variance_score as nse  # for the NSE error
from sklearn.metrics import mean_squared_error as MSE  # for the NSE error
import sklearn.metrics as sk
import matplotlib.pyplot as plt
import os  #to change working directory
#os.getcwd()
os.chdir('C:\\Users\\Mostafa\\Desktop\\My Files\\thesis\\My Thesis\\Data_and_Models\\Data\\23Oct')
import sqlite3  
db=sqlite3.connect('database\\final2\\rainfall_final2.db')
cursor=db.cursor()
#dbf=sqlite3.connect('database\\final2\\rainfall_final2.db')
#cursorf=dbf.cursor()
#from termcolor import colored
station=['Cojutepeque','Guadalupe','Ilobasco','Ilopango','Jerusalem',
     'La_Cima','Panchimalco','Puente_Viejo','San_Vicente','Tepezontes','Verapaz',
     'Zacatecoluca','Belloso','Hacienda_Melara','Melara','San_Vicente_Hidro',
     'Santiago_Nonualco','Tonacatepeque']
import sys
sys.path.append("C:\\Users\Mostafa\\Desktop\\My Files\\thesis\\My Thesis\\Data_and_Models\\Data\\23Oct\\functions")
from nearest_station import nearest_station
nearest=nearest_station(station,1)
second_nearest=nearest_station(station,2)
third_nearest=nearest_station(station,3)
#%%
j=0
cursor.execute("select date from "+station[j])
date=cursor.fetchall()
date=[i[0] for i in date]
df=pd.DataFrame(index=date)
cursor.execute("select precipitation from "+station[j])
p1=cursor.fetchall()
p1=[i[0] for i in p1]
df[station[j]]=p1

cursor.execute("select precipitation from "+nearest[j])
p2=cursor.fetchall()
p2=[i[0] for i in p2]
df[nearest[j]]=p2

cursor.execute("select precipitation from "+second_nearest[j])
p3=cursor.fetchall()
p3=[i[0] for i in p3]
df[second_nearest[j]]=p3
#%%plot
df.plot(subplots=True, figsize=(18, 8));
plt.legend(loc='best')
plt.xticks(rotation='vertical')
#%%correlation
# take the second and third columns (nearest and second nearest)
train_input_var=df.loc['2012-06-14 19:00:00':'2014-06-14 19:00:00'].as_matrix()[:,[1,2]]
train_output_var=df.loc['2012-06-14 19:00:00':'2014-06-14 19:00:00'].as_matrix()[:,[0]]
valid_input_var=df.loc['2014-06-14 19:15:00':'2015-09-21 16:30:00'].as_matrix()[:,[1,2]]
valid_output_var=df.loc['2014-06-14 19:15:00':'2015-09-21 16:30:00'].as_matrix()[:,[0]]
#x=input_var[:,0].tolist()
#y=output_var.tolist()
#y=[i[0] for i in y]
#correlation1=np.corrcoef(output_var,x)
#print(correlation1)

#plt.plot(input_var[:,1])
#plt.plot(p3)
#%% Train data transformation (normalisation)
#data transformation to cover the whole domain usin preprocessing function from sklearn liberary
# transformation of training input data
inp_scaler=preprocessing.StandardScaler() # assign the class standerscaler to inp_sclaer
inp_scaler.fit(train_input_var)            # compute mean & standard deviation of 
                                     # the data to use it in the standarization
train_inp_pp=inp_scaler.transform(train_input_var)  # using the method transform inside the class standerscaler

# transformation of training output data
train_output_var=train_output_var.reshape(-1,1) # because of an error
out_scaler=preprocessing.StandardScaler()
out_scaler.fit(train_output_var)
train_out_pp=out_scaler.transform(train_output_var)

print("scaled Mean{0}".format(np.round(np.average(train_inp_pp,axis=0),3)))# mean of all train input data
print("scaled Mean{0}".format(np.round(np.average(train_out_pp,axis=0),3))) # mean of all train out data
print("scaled Mean{0}".format(np.round(np.std(train_inp_pp,axis=0),3))) # mean of all train out data
print("scaled Mean{0}".format(np.round(np.std(train_out_pp,axis=0),3))) # mean of all train out data
#%% validation data transformation 

valid_inp_pp=inp_scaler.transform(valid_input_var)

valid_output_var=valid_output_var.reshape(-1,1)
valid_out_pp=out_scaler.transform(valid_output_var)

print(np.round(np.average(valid_inp_pp,axis=0),3))
print(np.round(np.average(valid_out_pp,axis=0),3))
#%% definition of MLP neural network
layer_size = (2000,)   # number of neurons
activation_function='tanh' #'logistic'
batch= 10  #epoch if epoch or batch = training set size so updating the weighting occurs only after running all 
# the data set if different updating the weights will occur after running the epoch and after running the whole data set
Ir= 'constant' # ir is the learning rate which is the update to the weights
initial_Ir=0.0001
convergence_iters=100000000
shuffle_train=True   #Whether to shuffle samples in each iteration. Only used when solver=’sgd’ or ‘adam’.

# setting of the MLP neural network 
reg_model=MLPRegressor(hidden_layer_sizes=layer_size,activation=activation_function,
                       batch_size=batch,learning_rate=Ir,
                       learning_rate_init= initial_Ir, max_iter=convergence_iters,
                       shuffle=shuffle_train)

# select the loss function
from RMSE import RMSE
reg_model.score=nse                 #sk.log_loss         #MSE             # RMSE

# feed the MLP with the input & output data
reg_model.fit(train_inp_pp,train_out_pp)

# read results of the MLP
training_results=reg_model.predict(train_inp_pp)
validation_result=reg_model.predict(valid_inp_pp)

# back-transforming of the validation & training results

training_results_bt=out_scaler.inverse_transform(training_results)
validation_result_bt=out_scaler.inverse_transform(validation_result)
#%%visualization
plt.figure(1,figsize=(18,10))
plt.plot( train_output_var,label="Obs")
plt.plot(training_results_bt,label="sim")
plt.grid()
plt.legend()
plt.xlabel("steps")
plt.ylabel("Precipitation mm")
plt.title("Training")
plt.show()

plt.figure(2,figsize=(18,10))
plt.plot(valid_output_var,label="Obs")
plt.plot(validation_result_bt, label="sim")
plt.grid()
plt.legend()
plt.xlabel("Steps")
plt.ylabel("Precipitation mm")
plt.title("Validation")
plt.show()
#%%
rmse_error=RMSE(train_output_var[:],training_results_bt,)
print ("Training data RMSE = " +str(rmse_error))
rmse_error=RMSE(train_output_var,training_results_bt,)
print ("validation data RMSE = " +str(rmse_error))