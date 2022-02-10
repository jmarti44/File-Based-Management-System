#inital class instantiation
from data import Database
database = Database()

def dbFunction(option):
    if option == '1':
        newPrefix = input('Enter the name of the desired csv file\n')
        database.create(newPrefix)
        print('Database closed by default after creation')
        menu = input('Press Enter to return to the main menu')
        displayMenu()
    elif option == '2':
        newPrefix = input('Enter the prefix name of the desired data file\n')
        database.open(newPrefix)
        menu = input('Press Enter to return to the main menu')
        displayMenu()
    elif option == '3':
        database.closeDB()
        menu = input('Press Enter to return to the main menu')
        displayMenu()
    elif option == '4':
        record = input('Enter desired record\n')
        database.displayRecord(record)
        menu = input('Press Enter to return to the main menu')
        displayMenu()
    elif option == '5':
        record =  input('Enter the desired ID you want to update\n')
        state = input('Enter state field\n')
        city  = input('Enter city field\n')
        name = input('Enter university name field\n')
        database.updateRecord(record,state,city,name)
        menu = input('Press Enter to return to the main menu')
        displayMenu()
    elif option  == '6':
        database.createReport()
        print('Report created')
        menu = input('Press Enter to return to main menu')
                


    elif option == '7':
        record = input('Enter the desired ID you want to add\n')
        state = input('Enter state field\n')
        city  = input('Enter city field\n')
        name = input('Enter university name field\n')
        database.addRecord(record,state,city,name)
        menu = input('Press Enter to return to the main menu')
        displayMenu()
    elif option == '8':
        record = input('Enter Record')
        database.updateRecord(record," "," "," ")
        menu = input('Press Enter to return to the main menu')
        displayMenu()
    elif option == '9':
        exit()
def displayMenu():
        
    option = ""
    while option!= '9':

        print('File-based Database System')

        print('1. ','Create new database')

        print('2. ','Open database')

        print('3. ','Close database')

        print('4. ','Display Record')

        print ('5. ','Update Record')
        
        print('6. ','Create Report')

        print('7. ','Add Record')

        print ('8. ','Delete Record')

        print('9. ','Exit Program')

        option = input('Select from the following options\n')
        dbFunction(option)
    

displayMenu()



    







































# print("Intializing Database and writing records for Test.data")
# secondPath = "colleges.csv"
# database.create(secondPath)









# testRecords = [0,9,5]

# for record in testRecords:
#     print('reading record ',record)
#     database.readRecord(record,newFile)

# print('\n')



















