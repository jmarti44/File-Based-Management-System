#inital class instantiation
from data import Database

print("Testing Writing and Reading individual records\n")
database = Database()

filePath = "test.csv"

print('Small test file ',filePath)
print("Intializing Database and writing records")
database.create(filePath)

newFile = "input.data"

testRecords = [0,9,5]

for record in testRecords:
    print('reading record ',record)
    database.readRecord(record,newFile)

print('\n')



















