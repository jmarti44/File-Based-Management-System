#inital class instantiation
from data import Database

print("Testing Writing and Reading individual records\n")
database = Database()

filePath = "test.csv"

print("Intializing Database and writing records for Test.data")
database.create(filePath)
print('Testing Opening Database')
print("Attempting to display Record 7...")
database.displayRecord(7)
database.open('test')
print("Attempting to display Record 7...")
database.displayRecord(7)














# print("Intializing Database and writing records for Test.data")
# secondPath = "colleges.csv"
# database.create(secondPath)









# testRecords = [0,9,5]

# for record in testRecords:
#     print('reading record ',record)
#     database.readRecord(record,newFile)

# print('\n')



















