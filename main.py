#inital class instantiation
from data import Database

print("Testing Writing and Reading individual records\n")
database = Database()

filePath = "test.csv"


print("Intializing Database and writing records for Test.data")
database.create('test')
database.create('colleges')
database.open('test')
database.open('colleges')

# print("Attempting to display Record 7...")
# database.displayRecord(7)
# print('Testing Opening Database')
# database.open('test')
# print("Attempting to display 100858...")
# database.displayRecord('100858')
# # # print('sorted records:' ,database.numSortedRecords)
# # # print('closing database')
# # # database.closeDB()
# # # database.displayRecord(100858)
# # # print('sorted records:' ,database.numSortedRecords)
# # # # database.readRecord(9)
# # # #215114,Pennsylvania,Langhorne,Cairn_University
# # # database.open('test')
# print('Testing addRecord')
database.addRecord('215114','Ark','Langhorne','Langhorne_Cairn_University')
database.closeDB()
database.open('colleges')
database.addRecord('200000','testing','testing','!')
# database.addRecord('111111','Jo','Jo','Jo')
# database.addRecord('111111','Jo','Hello','DDd')
# database.create('colleges')



# database.updateRecord('100858','Jose','Jose','Jose')
# database.deleteRecord('22')
# database.updateRecord('215114','Tom','Tom','Tom')
# database.updateRecord('111111','Mom',"Mom",'!!')
# database.addRecord('215114','Ya','Hello','Mommy')
# database.updateRecord('111111','Tom','Tom','Name')




#214704,Pennsylvania,Reading,Penn_State_University_Berks
# database.addRecord(214704,'Pennsylvania','Reading','Penn_State_University_Berks')
# database.updateRecord('215114','Texas','!','Jose')

















# print("Intializing Database and writing records for Test.data")
# secondPath = "colleges.csv"
# database.create(secondPath)









# testRecords = [0,9,5]

# for record in testRecords:
#     print('reading record ',record)
#     database.readRecord(record,newFile)

# print('\n')



















