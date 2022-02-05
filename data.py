from asyncore import write
from collections import defaultdict
import os.path 


import csv
from re import M

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



        self.record = {}
        self.prefix = None
        self.database = False
        self.files =set()
    
    #creating intial database with filepath constructor
    def create(self,filePath):

        self.filename = filePath
        self.prefix =os.path.splitext(filePath)[0]
        self.files.add(self.prefix)
        self.setMaxFields()
    
        #adjusting records and writing them to .data one line at a time
        with open(self.filename,'r') as q:
            for _ in q.readlines():
                record =  _.rstrip('\n').split(',')
                adjustedRecord = self.adjustRecordLength(record)
                id = adjustedRecord[0]
                state = adjustedRecord[1]
                city = adjustedRecord[2]
                name = adjustedRecord[3]
                self.filename = '{}.data'.format(self.prefix)
                self.writeRecord(id,state,city,name)
        q.close
        self.closeDB()

    def setMaxFields(self):
        idLengths = []
        stateLengths =[]
        cityLengths = []
        nameLengths = []
        with open(self.filename,'r') as f:
            #getting lengths of all records
            for i in f.readlines():
                record =  i.rstrip('\n').split(',')
                currentID = record[0]
                currentState = record[1]
                currentCity = record[2]
                currentName = record[3]
                idLengths.append(len(currentID))
                stateLengths.append(len(currentState))
                cityLengths.append(len(currentCity))
                nameLengths.append(len(currentName))
        f.close()
        self.maxIDSize = max(idLengths)
        self.maxStateSize = max(stateLengths)
        self.maxCitySize = max(cityLengths)
        self.maxNameSize = max(nameLengths)

    def open(self,file):
        if file in self.files:
            self.prefix = file
            print('calling open')
            self.reinitalizeDB(file)
            self.filename = '{}.data'.format(self.prefix)
    def reinitalizeDB(self,file):
        self.database = True
        configFile = "{}.config".format(self.prefix)

        rec =  []
        #getting numsorted and overflow
        with open(configFile,'r') as q:
            for row in q.readlines():
                record =  row.rstrip('\n').split(' ')
                rec.append(record)
        
        self.numSortedRecords = int(rec[0][-1])
        self.numOverflowRecords = int(rec[1][-1])
        self.RECORD_SIZE = int(rec[2][-1])


 






        # filePointer = open('test.csv','r')
        # self.filename = filename
        # if not os.path.isfile(self.filename):
        #     print(str(self.filename)+" not found")

        
    
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
        self.database = False
        self.prefix = None
        #instance variables
        self.numSortedRecords = 0                         
        self.numOverflowRecords = 0
        self.RECORD_SIZE = 0


    
    def isOpen(self):
        return self.database

    
    def getRecordSize(self):
        with open(self.filename,'r') as q:
            for i in q.readlines():
                self.RECORD_SIZE = len(i)
        q.close

        return self.RECORD_SIZE
        
    def readRecord(self,recordNum):
        # self.filename = indicated_file
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
        self.record = dict({"ID":id,"State":state.strip(),"City":city.strip(),"Name":name.strip()})

        return flag

    def writeRecord(self,id,state,city,name):
        newRecord = id  + "," + state + "," + city   + ","  + name +  "\n"
        try:
            with open('{}.data'.format(self.prefix), 'a') as f:
                f.write(newRecord)
        except FileNotFoundError:
            print("The 'docs' directory does not exist")
        self.numSortedRecords+=1
        self.RECORD_SIZE = self.getRecordSize()

        with open ("{}.config".format(self.prefix), 'w') as config:
            config.write("Number of Sorted Records: {0}".format(self.numSortedRecords))
            config.write('\n')
            config.write('Number of Overflow Records: {}'.format(self.numOverflowRecords))
            config.write('\n')
            config.write('Record Size: {}'.format(self.RECORD_SIZE))
        with open ("{}.overflow".format(self.prefix), 'w') as overflow:
            overflow.write("testing purposes")
        
    def overwriteRecord(self,file_ptr,recordNum,id,state,city,name):
        flag = self.isOpen()
        if flag == True:
            pass
        else:
            print('Given database is not open')
    def appendRecord(self,id,state,city,name):
        flag = self.isOpen()
        if flag == True:
            pass
        else:
            print('Given database is not open')
        #Binary Search by record id
    def binarySearch (self, input_ID):
        
        low = 0
        high = self.RECORD_SIZE - 1
        self.found = False

        while high >= low:

            self.middle = (low+high)//2
            self.readRecord(self.middle)
            # print(self.record)
            mid_id = self.record["ID"]
            print(mid_id)
            
            if int(mid_id) == int(input_ID):
                self.found = True
                break
            elif int(mid_id) > int(input_ID):
                high = self.middle - 1
            elif int(mid_id) < int(input_ID):
                low = self.middle + 1
        print(self.record)
    

    def displayRecord(self,id):
        flag = self.isOpen()
        if flag == True:
            self.binarySearch(id)
        else:
            print('Database is not open')








