from asyncore import write
from collections import defaultdict
import os.path 


import csv

class Database:

    #default constructor
    def __init__(self):
        #instance variables
        self.numSortedRecords = 0                         
        self.numOverflowRecords = 0
        self.filename = ""
        self.RECORD_SIZE = 0


        #max field sizes
        self.maxIDSize = 0
        self.maxStateSize = 0
        self.maxCitySize = 0
        self.maxNameSize = 0
    
    #creating intial database with filepath constructor
    def create(self,filePath):
        idLengths = []
        stateLengths = []
        cityLenghts = []
        nameLengths = []
        self.filename = filePath
        with open(filePath,'r') as f:

            currentID = ""
            currentState =  ""
            currentCity = ""
            currentName = ""
            #getting lenghs of all records
            for i in f.readlines():
                record =  i.rstrip('\n').split(',')

                # currentID,currentState,currentCity,currentName = record#[currentID,currentState,currentCity, currentName]
                currentID = record[0]
                currentState = record[1]
                currentCity = record[2]
                currentName = record[3]
                idLengths.append(len(currentID))
                stateLengths.append(len(currentState))
                cityLenghts.append(len(currentCity))
                nameLengths.append(len(currentName))
            



            #setting max field size instance variables
            self.maxIDSize = max(idLengths)
            self.maxStateSize = max(stateLengths)
            self.maxCitySize = max(cityLenghts)
            self.maxNameSize = max(nameLengths)

            #adjusting records and writing them to .data one line at a time
            with open(self.filename,'r') as q:
                for _ in q.readlines():
                    record =  _.rstrip('\n').split(',')
                    adjustedRecord = self.adjustRecordLength(record)
                    id = adjustedRecord[0]
                    state = adjustedRecord[1]
                    city = adjustedRecord[2]
                    name = adjustedRecord[3]

                    self.writeRecord(id,state,city,name)
            q.close
        f.close

    def open(self,filename):
        filePointer = open('test.csv','r')
        self.filename = filename
        if not os.path.isfile(self.filename):
            print(str(self.filename)+" not found")

        
    
    #helper function to adjust all record lengths
    def adjustRecordLength(self,record):


        currentRecordIDLength =  len(record[0])
        currentRecordStateLength = len(record[1])
        currentRecordCityLength = len(record[2])
        currentRecordNameLength= len(record[3])

        #use format write to adjust record
        if currentRecordIDLength < self.maxIDSize:
            numSpaces = self.maxIDSize - currentRecordIDLength
            record[0] = record[0] + "{}".format(" "*numSpaces)


        if currentRecordStateLength < self.maxStateSize:
            numSpaces = self.maxStateSize - currentRecordStateLength   
            record[1] = record[1] + "{}".format(" "*numSpaces)


        if currentRecordCityLength  < self.maxCitySize:
            numSpaces = self.maxCitySize - currentRecordCityLength  
            record[2] = record[2] + "{}".format(" "*numSpaces)
        
        if currentRecordNameLength < self.maxNameSize:
            numSpaces = self.maxNameSize - currentRecordNameLength
            record[3] = record[3] + "{}".format(" "*numSpaces)
        
      
        return record

    #Binary Search by record id
    def binarySearch (self, input_ID):
        
        low = 0
        high = self.record_size - 1
        self.found = False

        while high >= low:

            self.middle = (low+high)//2
            self.getRecord(self.middle)
            # print(self.record)
            mid_id = self.record["ID"]
            
            if int(mid_id) == int(input_ID):
                self.found = True
                break
            elif int(mid_id) > int(input_ID):
                high = self.middle - 1
            elif int(mid_id) < int(input_ID):
                low = self.middle + 1
        
    def closeDB(self):
        self.data.close()

    
    def isOpen(self):
        pass
    
    def getRecordSize(self):
        with open(self.filename,'r') as q:
            for i in q.readlines():
                self.RECORD_SIZE = len(i)
    
        q.close
        return self.RECORD_SIZE
        
    def readRecord(self,recordNum,indicated_file):
        self.filename = indicated_file
        self.RECORD_SIZE = self.getRecordSize()
        



        self.data = open(self.filename,'r')
        id = state = city = name = "None"
        flag = False
        isopen = False
        try:
            self.data.closed
            if self.data.closed:
                isopen = False
            else:
                self.data = open(self.filename, 'r')
                isopen = True
        except AttributeError:
            isopen = False
        if isopen and recordNum >=0 and recordNum < self.RECORD_SIZE:
            self.data.seek(0,0)
            self.data.seek(recordNum*self.RECORD_SIZE)
            line = self.data.readline().rstrip('\n')
            flag = True
        if flag:
            record = line.split(',')
            id, state, city, name = record
            # print('id',id)
        recordMessage = "Record:" + str(recordNum) + " " + "ID:" + id + " "+ "State:" + state.strip() + " " + "City:" + city.strip() + " "+ "Name: " + name.strip()
        print(recordMessage) 
        
        return flag

    def writeRecord(self,id,state,city,name):
        #calling helper method to get max length of fields
        # newRecord = key + "," + id + "," + state + "," + city + "," + name + "\n"
        newRecord = id  + "," + state + "," + city   + ","  + name +  "\n"
        try:
            with open('input.data', 'a') as f:
                f.write(newRecord)
        except FileNotFoundError:
            print("The 'docs' directory does not exist")
        self.numSortedRecords+=1

        with open ("colleges_lf.config", 'w') as config:
            config.write("number of records being written into .data file: {0}".format(self.numSortedRecords))
        with open ("colleges_lf.overflow", 'w') as overflow:
            overflow.write("testing purposes")

    def overwriteRecord(self,file_ptr,recordNum,id,state,city,name):
        pass
    def appendRecord(self,id,state,city,name):
        pass
        #Binary Search by record id
    def binarySearch (self, input_ID):
        
        low = 0
        high = self.record_size - 1
        self.found = False

        while high >= low:

            self.middle = (low+high)//2
            self.getRecord(self.middle)
            # print(self.record)
            mid_id = self.record["ID"]
            
            if int(mid_id) == int(input_ID):
                self.found = True
                break
            elif int(mid_id) > int(input_ID):
                high = self.middle - 1
            elif int(mid_id) < int(input_ID):
                low = self.middle + 1
    







