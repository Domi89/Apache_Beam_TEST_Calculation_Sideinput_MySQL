import apache_beam as beam
from datetime import datetime
from datetime import date
from beam_mysql.connector.io import WriteToMySQL
from pathlib import Path

#initiating pipeline
p = beam.Pipeline()



#A Parameters--------------------------------------------------------------

#A1 Parameters input file

#As the unpaid_invoices file exports all invoice related information, there are a lot of colomns
#Those parameters show in which colomn the data comes from
betragP = 65
invoiceDate = 69
dueDate = 68
kundenNrP = 84
kundenNameP =75

#A2 Parameters to calculate creditworthiness
#those are the parameters to calculate if future orders will be blocked or not
credit_limit_exhaution_max= 1.2
due_in_percent_max= 0.3
oldest_invoice_max= 30


#A3 Filepaths
# The Path function can not be used for the Beam transform ReadFromText
# it must be a String
unpaid_invoices_file = "C:/Users/Administrator/Documents/Tobit/" \
                            "Experiment_1_creditworthiness/input_files/unpaid_invoices.csv"

#to iterate threw the side_input, the Path has to be defined
side_input_file = Path("C:/Users/Administrator/Documents/Tobit/" \
                       "Experiment_1_creditworthiness/input_files/side_input_creditlimit.csv")

#A4 MySQL
#MySQL database connection
write_to_mysql = WriteToMySQL(
        host="localhost",
        database="beamtest",
        table="apache_beam_v1",
        user="root",
        password="TEST",
        port=12000,
        batch_size=0,
)


#B Sideinput----------------------------------------------------------------------

#this paragraph puts the side input file in a list to get
#further access on it
side_input_credit_limit_list = list()
with open (data_folder, 'r') as my_file:
    next(my_file)
    for line in my_file:
        
        splitet_elements = line.rstrip().split(";")
        
        #The client number from this system is slightly different,
        #there is an additional c which has to be removed
        clientNumber = int(splitet_elements[0].replace("C",""))
        
        #Credit limit format is not usable as an int value
        # ,00 and space has to be removed
        creditLimit = int(splitet_elements[1].replace(",00","").replace(" ",""))
        
        side_input_credit_limit_list.append([clientNumber, creditLimit])



#C Pipelines----------------------------------------------------------------------

# this pipeline reads the unpaid invoices file, extract the needed 
# data for further transformation. Than it adss the due amount, this 
# is calculated by the due date
# Output: Every output element reflects one invoice with 
# the following data structure: 
# [Client number, client name, invoice date, invoice due date, amount, due amount]

p0_firstStep = (
    p
    |beam.io.ReadFromText(unpaid_invoices_file, skip_header_lines=True)
    |beam.Map(lambda element: element.split(';'))
    |beam.Map(cleanInput)
    |beam.Map(addDueAmount)

)


# The following pipeline takes the p0_firstStep pipeline as input.
# With the Map function it returns the needed elements for the next
# steps. The return must be key (element[0]) and a value (tuple of
# element[4] + [5]), because the next step (GroupByKey) can only handle
# two elements. This function groups the elements by key (client number).
# Next it calculates the due amounts in percentage of the total open 
# invoices amount.
# The output of this pipeline is sturctured as follows:
# (Client number, {'Due_In_Percent_Of_Total': calculated value})
# ! values marked like this: { : } are called dictionaries
p1_dueInPercent = (
    p0_firstStep   
    |beam.Map(lambda element: (element[0], (element[4], element[5])))
    |beam.GroupByKey()
    |beam.Map(berechnungProzent)

)


# The following pipeline takes the p0_firstStep pipeline as input.
# With several transformation it calculates the oldest 
# invoice and returns the days since when it is overdue.
# if there are no overdue invoices, the return is equal to zero.
# The output of this pipeline is sturctured as follows:
# (Client number, {'Oldest_Invoice': calculated value})
p1_oldestInvoice = (
    p0_firstStep
    |beam.Filter(lambda element: element[4]>0)
    |beam.Filter(removeNotDue)  
    |beam.Map(cleanTableDueDate) 
    |'GroupBy' >> beam.GroupByKey()
    |beam.Map(calculateOldestInvoiceDays)
)


# The following pipeline takes the p0_firstStep pipeline as input.
# With several transformation it calculates the total open invoices
# in percentage of the credit limit. 
# if there is no credit limit set by the system, the return value 
# is equal to -10.
# The output of this pipeline is sturctured as follows:
# (Client number, {'Credit_Limit_Exhaustion': calculated value})
p1_creditlimitAchievement = (
    p0_firstStep
    |beam.Map(lambda element: (element[0], element[4]))
    |beam.CombinePerKey(sum)
    |beam.Map(lambda element: (element[0], round(element[1],2)))
    |beam.Map(totalInPercOfCreditlimit)

)

# The following pipeline takes the p0_firstStep pipeline as input.
# It groups all amounts by customer number. With the last
# function it sums up the total open invoices and returns:
# (Client number, {'Total_Open': calculated value})
p1_totalOpenInvoice = (
    p0_firstStep   
    |beam.Map(lambda element: (element[0], element[4]))
    |'combines the invoice amounts'>>beam.GroupByKey()
    |beam.Map(addUpAmountsInvoice)  
)


# The following pipeline takes the p0_firstStep pipeline as input.
# It groups all amounts by customer number. With the last
# function it sums up the total open invoices and returns:
# (Client number, {'Total_Open': calculated value})
p1_totalDueAmount = (
    p0_firstStep   
    |beam.Map(lambda element: (element[0], element[5]))
    |'combines the due amounts'>>beam.GroupByKey()
    |'adds up due amounts' >> beam.Map(addUpAmountsDue)
)
  

# The following pipeline takes the following pipelines as input:
# (p1_dueInPercent, p1_oldestInvoice, p1_creditlimitAchievement, 
# p1_totalOpenInvoice, p1_totalDueAmount)
# With the CoGroupByKey() transform it transforms all those PCollection
# to one PCollection. IMPORTANT: this is only possible if all the 
# pipelines have the same structure.

# the formatFinalP4 is excetuted to format the elements in a dictionary,
# which is needed to write the elements to the MySQL database
# this function also adds, additional information such as credit limit
# and upload datettime

# the function calculationBlockedFutureOrders calculates
# if future orders are blocked (1) or not (0). This information
# will then be added to the dictionary.


# The last transform writes the data to MySQL in the following format (dictionary)
# IMPORTANT: Dictionary names and datatype must be equal to the MySQL table:
# {'KdNr': 103332, 'Due_In_Percent_Of_Total': 1.0, 'Oldest_Invoice': 931, 
# 'Credit_Limit_Exhaustion': value1, 'Credit_Limit': value2, 'Total_Open': value3, 
# 'Total_Due': value4, 'Import_Date': value5, 
# 'Blocked': value6}

p4_combined_results = (
    (p1_dueInPercent, p1_oldestInvoice, p1_creditlimitAchievement, p1_totalOpenInvoice, p1_totalDueAmount)
    |beam.CoGroupByKey()
    |beam.Map(formatFinalP4)
    |beam.Map(calculationBlockedFutureOrders)
    | "WriteToMySQL" >> write_to_mysql
)



#D Functions------------------------------------------------------------------------

# the functions are not explained in detail as they should be clear

#cleans the Input, put Date into Datetime object
#removes ' from the value of the invoice
#returns the wanted elements in a array: client_number, client name, invoice_date, due_date, value
def cleanInput(element):
    
    datetime_1 =element[invoiceDate]
    invoiceDateObject = datetime.strptime(datetime_1, '%d.%m.%Y')
    
    datetime_2 =element[dueDate]
    dueDateObject = datetime.strptime(datetime_2, '%d.%m.%Y')
    
    betrag = element[betragP].replace("'","")
    betrag = float(betrag)
    
    kdNr = int(element[kundenNrP].replace("'",""))
        
    return [kdNr, element[kundenNameP], invoiceDateObject, dueDateObject, betrag]
    

    
def addDueAmount(element):
    
    today = datetime.today()
    
    if element[3] > today or element[4]<0:
        element.append(0)
    else:
        element.append(element[4])       

    return element



def berechnungProzent(element):
    
    verfallen = 0
    offen = 0

    for i in element[1]:
        offen += i[0]

        verfallen += i[1]

    prozentualerVerfall = round(verfallen / offen,2)
    
    return element[0], {"Due_In_Percent_Of_Total": prozentualerVerfall}



def removeNotDue(element):
    today = datetime.today()
    if element[3] < today:
        return element
    else:
        return today
    
def cleanTableDueDate(element):
    #print(element)
    return (element[0], element[3])


# calculates the oldest invoice
def calculateOldestInvoiceDays(element):
    today = datetime.today()
    copy = datetime.today()
  
    for i in element[1]:
        if i < copy:
            copy = i

    difference = today - copy

    return element[0], {"Oldest_Invoice": difference.days}


def addUpAmountsInvoice(element):
    total = 0
    for i in element[1]:
        total += i
    return element[0], {"Total_Open": round(total, 2)}
    
    
def addUpAmountsDue(element):
    total = 0
    for i in element[1]:
        total += i
    return element[0], {"Total_Due": round(total, 2)}
    

def totalInPercOfCreditlimit(element):
    
    credit_limit = 0
    
    for e in side_input_credit_limit_list:
        if e[0] == element[0]:
            credit_limit = e[1]
    
    if credit_limit != 0:
        calculation = round(element[1]/credit_limit,2)
    else:
        #if there is no creditlimit set, value -10
        calculation = -10.0

    return element[0], {"Credit_Limit_Exhaustion": calculation}
            

def calculationBlockedFutureOrders(element):

    #if blocked 1, else 0
    
    blocked = 0       
        
    if not element.get('Due_In_Percent_Of_Total') == None:
        Due_In_Percent_Of_Total = (element.get('Due_In_Percent_Of_Total'))
    if not element.get('Oldest_Invoice') == None:
        Oldest_Invoice = (element.get('Oldest_Invoice'))
    if not element.get('Credit_Limit_Exhaustion') == None:
         Credit_Limit_Exhaustion = (element.get('Credit_Limit_Exhaustion'))
         
    if Credit_Limit_Exhaustion > credit_limit_exhaution_max:
        blocked = 1
    
    if Oldest_Invoice > oldest_invoice_max and Due_In_Percent_Of_Total > due_in_percent_max:
        blocked = 1
    
    element['Blocked'] = blocked
    
    return element




def formatFinalP4(element):

    
    KdNr = element[0] 
    uploadTime = datetime.today()
    
    # extracts the needed data from the input
    for i in element[1]:
        if not i[0].get('Due_In_Percent_Of_Total') == None:
            Due_In_Percent_Of_Total = (i[0].get('Due_In_Percent_Of_Total'))
        if not i[0].get('Oldest_Invoice') == None:
            Oldest_Invoice = (i[0].get('Oldest_Invoice'))
        if not i[0].get('Credit_Limit_Exhaustion') == None:
            Credit_Limit_Exhaustion = (i[0].get('Credit_Limit_Exhaustion'))
        if not i[0].get('Total_Open') == None:
            total_open = (i[0].get('Total_Open'))
        if not i[0].get('Total_Due') == None:
            total_due = (i[0].get('Total_Due'))

    credit_limit = 0
    #returns -10 if no credit limit is defined
    for e in side_input_credit_limit_list:
        if e[0] == KdNr:
            credit_limit = e[1]
    if credit_limit == 0:
        credit_limit = -10

      
    returnValue = {'KdNr':KdNr,
                    'Due_In_Percent_Of_Total':Due_In_Percent_Of_Total,
                    'Oldest_Invoice':Oldest_Invoice,
                    'Credit_Limit_Exhaustion':Credit_Limit_Exhaustion,
                    'Credit_Limit': credit_limit,
                    'Total_Open': total_open,
                    'Total_Due': total_due,
                    'Import_Date':uploadTime
                  }
    
    return returnValue



#E RUN the pipeline --------------------------------------------------------------

p.run().wait_until_finish()
