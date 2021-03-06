from asyncore import write
from collections import defaultdict
from decimal import Overflow
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


        #key instance variables for record and file information
        self.record = None
        self.prefix = None
        self.database = False
        self.files =set()
        self.filePointer = ""
        self.foundRecordNum = ""
    
    #creating intial database with filepath constructor
    def create(self,filePath):
        #prefix takes name of the file without any extensions
        self.filename = filePath + '.csv'
        self.prefix =os.path.splitext(filePath)[0]

        #self.files set tp keep track of files
        self.files.add(self.prefix)
        self.setMaxFields()
    
        #adjusting records and writing them to .data one line at a time
        with open(self.filename,'r') as q:
            i = 0
            for _ in q.readlines():
                record =  _.rstrip('\n').split(',')
                #[id,state,city,name]
                #record.insert(0,i)
                adjustedRecord = '{}'.format(record)
                adjustedRecord = self.adjustRecordLength(record)
                id = adjustedRecord[0]
                state = adjustedRecord[1]
                city = adjustedRecord[2]
                name = adjustedRecord[3]
    
                self.filename = '{}.data'.format(self.prefix)
                self.writeRecord(id,state,city, name)
                i+=1
                
        q.close
        self.closeDB()

    #setting maxFields
    def setMaxFields(self):
        idLengths = []
        stateLengths =[]
        cityLengths = []
        nameLengths = []
        keyLengths = []
        with open(self.filename,'r') as f:
            #getting lengths of all records
            key = 0
            for i in f.readlines():
                record =  i.rstrip('\n').split(',')
                record.insert(0,key)
                currentKey = record[0]
                currentID = record[1]
                currentState = record[2]
                currentCity = record[3]
                currentName = record[4]
                idLengths.append(len(currentID))
                stateLengths.append(len(currentState))
                cityLengths.append(len(currentCity))
                nameLengths.append(len(currentName))
                keyLengths.append(len(str(currentKey)))
        f.close()
        self.maxKeySize = max(keyLengths)
        self.maxIDSize = max(idLengths)
        self.maxStateSize = max(stateLengths)
        self.maxCitySize = max(cityLengths)
        self.maxNameSize = max(nameLengths)

    #checking isOpen attribute
    def open(self,file):
        if self.isOpen() == True:
            print ('Another database is curretnly opened')
        elif file in self.files:
            self.prefix = file
            self.reinitalizeDB(file)
            self.filename = '{}.data'.format(self.prefix)

    #helper function for setting instance varaibles from config file
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
        q.close()
    
    #helper function to adjust all record lengths according to maxFields
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
        
        #returns adjuted record
        return record

    #resets instance vaibales and sets database  to false    
    def closeDB(self):
        self.database = False
        print(self.prefix,' database closed')
        self.prefix = None
        #instance variables
        self.numSortedRecords = 0                         
        self.numOverflowRecords = 0
        self.RECORD_SIZE = 0

    #return self.database attribute
    def isOpen(self):
        return self.database

    #helper function for getting record size
    def getRecordSize(self):

        with open(self.filename,'r') as q:
            for i in q.readlines():
                self.RECORD_SIZE = len(i)
        q.close
        return self.RECORD_SIZE

    #filepointer(extension) and filename(name) parameters used to account for .data and.overflow
    def readRecord(self,recordNum,filePointer):
        self.filename = self.prefix + "."+ filePointer
        self.data = open(self.filename,'r+')
        id = state = city = name = "None"
        flag = False
        if recordNum >=0 and recordNum < self.numSortedRecords:
            self.data.seek(0,0)
            self.data.seek(recordNum*self.RECORD_SIZE)
            line = self.data.readline().rstrip('\n')
            flag = True
        if flag:
            record = line.split(',')
            id, state, city, name = record
        self.record = dict({"ID":id,"State":state.strip(),"City":city.strip(),"Name":name.strip()})
        self.foundRecordNum = recordNum

    #for writing into intil .data
    def writeRecord(self,id,state,city,name):
        newRecord = id  + "," + state + "," + city   + ","  + name +  "\n"
        try:
            with open('{}.data'.format(self.prefix), 'a') as f:
                f.write(newRecord)
        except FileNotFoundError:
            print("The 'docs' directory does not exist")
        self.numSortedRecords+=1
        self.RECORD_SIZE = self.getRecordSize()
        self.updateConfig

        #creating config and overflow files
        with open ("{}.config".format(self.prefix), 'w') as config:
            config.write("Number of Sorted Records: {0}".format(self.numSortedRecords))
            config.write('\n')
            config.write('Number of Overflow Records: {}'.format(self.numOverflowRecords))
            config.write('\n')
            config.write('Record Size: {}'.format(self.RECORD_SIZE))
        with open ("{}.overflow".format(self.prefix), 'w') as overflow:
            overflow.write("")
            
    #flag used to check if database is open
    def updateRecord(self,id,state,city,name):
        flag = self.isOpen()
        if flag == True:
            find = self.findRecord(id)
            if find == True:
                self.overwriteRecord(id,state,city,name,self.filePointer)
            else:
                print("Record not found")
        else:
            print('Given database is not open')


    def deleteRecord(self,id):
        flag = self.isOpen()
        if flag == True:
            find = self.findRecord(id)
            if find == True:
                self.overwriteRecord(id," ", " "," ",self.filePointer)
            else:
                print('Record not found')
            
        else:
            print('Given database is not open')
    

    def overwriteRecord(self,id,state,city,name,filePointer):
        
        newRecord = id + ',' + state + ','+ city + ',' + name
        newRecord = newRecord.split(',')
        adjustedRecord = self.adjustRecordLength(newRecord)
        currentID = adjustedRecord[0]
        currentState = adjustedRecord[1]
        currentCity = adjustedRecord[2]
        currentName = adjustedRecord[3]
        newRecord = currentID  + "," + currentState + "," + currentCity   + ","  + currentName +  "\n"
        self.filename = self.prefix + "." + filePointer
        if filePointer == "data":
            self.foundRecordNum +=1
            self.data = open('{}.data'.format(self.prefix),'r+')
            self.data.seek(0,0)
            self.data.seek(int(self.foundRecordNum-1)*self.RECORD_SIZE)
            self.data.write(newRecord)
        elif filePointer == "overflow":
        
            self.foundRecordNum +=1
            self.data = open('{}.overflow'.format(self.prefix),'r+')
            self.data.seek(0,0)
            self.data.seek(int(self.foundRecordNum-1)*self.RECORD_SIZE)
            self.data.write(newRecord)
        self.data.close()
    
    #helper function for length of overflow file
    def lengthOfOverflow(self):
        filePath = '{}.overflow'.format(self.prefix)
        overFlowFile = open(filePath,'r+')
        size = 0
        for i in overFlowFile.readlines():
            size = len(i)
        return size


    def addRecord(self,id,state,city,name):
        flag = self.isOpen()
        if flag == True:
            if self.lengthOfOverflow == 0:
                currentRecord = id + ',' + state + ','+ city + ',' + name
                currentRecord = currentRecord.split(',')
                adjustedRecord = self.adjustRecordLength(currentRecord)

                currentID = adjustedRecord[0]
                currentState = adjustedRecord[1]
                currentCity = adjustedRecord[2]
                currentName = adjustedRecord[3]

                self.appendRecord(currentID,currentState,currentCity,currentName)
            else:
                find = self.findRecord(id)
                if find == True:
                    print('Record already in database')
                else:
                    currentRecord = id + ',' + state + ','+ city + ',' + name
                    currentRecord = currentRecord.split(',')
                    adjustedRecord = self.adjustRecordLength(currentRecord)

                    currentID = adjustedRecord[0]
                    currentState = adjustedRecord[1]
                    currentCity = adjustedRecord[2]
                    currentName = adjustedRecord[3]

                    self.appendRecord(currentID,currentState,currentCity,currentName)
        else:
            print('Given database is not open')

    
    def appendRecord(self,id,state,city,name):
        newRecord = id  + "," + state + "," + city   + ","  + name +  "\n"

        overflowFile = open('{}.overflow'.format(self.prefix),'a')
        overflowFile.write(newRecord)
        overflowFile.close()
        #updating config file
        configFile = '{}.config'.format(self.prefix)
        self.updateConfig(configFile)

    #helper function for updating config file when appending to overflow file
    def updateConfig(self,configFile):
        # overflowRecords = int(rec[1][-1])
        rec =  []
        with open(configFile,'r') as c:
            for row in c.readlines():
                record =  row.rstrip('\n').split(' ')
                rec.append(record)
        self.numSortedRecords = int(rec[0][-1])
        currentOverflow= int(rec[1][-1])
        self.RECORD_SIZE = int(rec[2][-1])
        self.numOverflowRecords = currentOverflow + 1
        with open (configFile,'w') as d:
            d.write("Number of Sorted Records: {0}".format(self.numSortedRecords))
            d.write('\n')
            d.write('Number of Overflow Records: {}'.format(self.numOverflowRecords))
            d.write('\n')
            d.write('Record Size: {}'.format(self.RECORD_SIZE))
   

    #performs a binary search and linear search and sets self.filePointer(extension)
    def findRecord(self,id):
        
        binary = self.binarySearch(id,'data')
        linear = self.linearSearch(id)


        if binary == True:
            self.filePointer = "data"
        elif linear == True:
            self.filePointer = "overflow"
        return binary or linear

    #linear search helper function starting at record 0
    def linearSearch(self,id):
        currentRecord = 0
        overflowCount = 1
        flag = True
        if self.record['ID'] == "None":
            self.record['ID'] = '0'
        while int(id)!= int(self.record['ID']):
            if overflowCount > self.numOverflowRecords:
                flag = False
                break
            self.readRecord(currentRecord,"overflow")
            currentRecord+=1
            overflowCount+=1
        return flag

    #binary search helper function
    def binarySearch (self, input_ID,filePointer):
        
        low = 0
        high = self.numSortedRecords
        self.found = False
        while high >= low:

            self.middle = (low+high)//2
            self.middle 
            self.readRecord(self.middle,filePointer)
            
        
            mid_id = self.record["ID"]
            #edge case: mid_id falls at the end 
            if mid_id ==  "None":
                break
            if int(mid_id) == int(input_ID):
                self.found = True
                break
            elif int(mid_id) > int(input_ID):
                high = self.middle - 1
            elif int(mid_id) < int(input_ID):
                low = self.middle + 1
        return self.found

    def displayRecord(self,id):
        flag = self.isOpen()
        if flag == True:
            find = self.findRecord(id)
            if find == True:
                recordID = self.record['ID']
                state = self.record['State']
                city = self.record['City']
                name = self.record['Name']
                record = recordID + " " + state + " " + city + " " + name
                print(record)


            else:
                print('Not a valid record')
        else:
            print('Database is not open')

    def createReport(self):
        flag = self.isOpen()
        if flag == True:
            row = 0

            while row < 10:
                self.readRecord(row,'data')
                recordID = self.record['ID']
                state = self.record['State']
                city = self.record['City']
                name = self.record['Name']
                newRecord = recordID  + "," + state + "," + city   + ","  + name +  "\n"
                with open('{}.txt'.format(self.prefix + " " + "report"), 'a') as f:
                    f.write(newRecord)
                row+=1
        else:
            print('Database is not open')
            