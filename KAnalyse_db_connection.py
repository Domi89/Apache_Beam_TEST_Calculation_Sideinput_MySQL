import apache_beam as beam
from datetime import datetime
from datetime import date
from beam_mysql.connector.io import WriteToMySQL

p = beam.Pipeline()

write_to_mysql = WriteToMySQL(
        host="localhost",
        database="beamtest",
        table="apache_beam_v1",
        user="root",
        password="Bucici80pure+",
        port=3306,
        batch_size=0,
)



side_input_credit_limit_list = list()
with open (r'C:\Users\Administrator\Documents\so\projectV1\side_input_creditlimit.csv', 'r') as my_file:
    next(my_file)
    for line in my_file:
        
        splitet_elements = line.rstrip().split(";")
        
        #KdNr remove C and transform to int
        element1 = int(splitet_elements[0].replace("C",""))
        
        #Credit limit, remove ,00 and space and transform to int
        
        element2 = int(splitet_elements[1].replace(",00","").replace(" ",""))
        
        side_input_credit_limit_list.append([element1, element2])



#Parameters
betragP = 65
invoiceDate = 69
dueDate = 68
kundenNrP = 84
kundenNameP =75

credit_limit_exhaution_max= 1.2
due_in_percent_max= 0.3
oldest_invoice_max= 30

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
    
#appends the value of the invoice value if the invoice is due
def addDueAmount(element):
    
    today = datetime.today()
    
    if element[3] > today or element[4]<0:
        element.append(0)
    else:
        element.append(element[4])       

    return element

#def tuple_sum(elements):
#    return(elements[0],sum(elements[1][0]),sum(elements[1][1]))   

def berechnungProzent(element):
    
    verfallen = 0
    offen = 0
#    print(element[0])
    for i in element[1]:
        offen += i[0]
#        print('offen '+str(i[0]))
        verfallen += i[1]
#        print('verfallen'+str(i[1]))

    prozentualerVerfall = round(verfallen / offen,2)
    
     #| "ReadFromInMemory" >> beam.Create([{"kdNr": "546512","id": "546", "art": "kred", "val": "4.5"}])
    
#    print((element[0], {"Due_In_Percent_Of_Total": prozentualerVerfall}))
    
    return element[0], {"Due_In_Percent_Of_Total": prozentualerVerfall}
    
#    print(prozentualerVerfall)

def removeNotDue(element):
    today = datetime.today()
    if element[3] < today:
#        print(element)
        return element
    else:
        return today
    
def cleanTableDueDate(element):
    #print(element)
    return (element[0], element[3])

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
           
           # break
    
    if credit_limit != 0:
        calculation = round(element[1]/credit_limit,2)
    else:
        #if there is no creditlimit set, value -10
        calculation = -10.0

    return element[0], {"Credit_Limit_Exhaustion": calculation}
            
   # return element[0], ({"Credit_Limit_Exhaustion": calculation}, {"Credit_Limit": credit_limit})

#---------------------------------------------------------------------------------------------------------------

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
   # if not element.get('Oldest_Invoice') == None:
   #            print(element.get('Oldest_Invoice'))       
       
   # print(Credit_Limit_Exhaustion, blocked)   
    
    element['Blocked'] = blocked
    
    return element
   # print(element)
    
    #credit_limit_exhaution_max: 1.2
    #due_in_percent_max: 0.3
    #oldest_invoice_max: 30
  
     #([{'Due_In_Percent_Of_Total': 1.0}], [{'Oldest_Invoice': 930}], 
     # [{'Credit_Limit_Exhaustion': 7.2159}], [{'Total_Open': 14431.76}], [{'Total_Due': 14431.76}])  
        

p0_firstStep = (
    p
    |beam.io.ReadFromText(r'C:\Users\Administrator\Documents\so\projectV1\import.csv', skip_header_lines=True)
    |beam.Map(lambda element: element.split(';'))
    |beam.Map(cleanInput)
)

p0_verfall = (
    p0_firstStep
    |beam.Map(addDueAmount)
)

p1_verfallInProzent = (
    p0_verfall   
    |beam.Map(lambda element: (element[0], (element[4], element[5])))
    |beam.GroupByKey()
    |beam.Map(berechnungProzent)
    
   # |beam.Map(lambda element: print(element)) 
   # |beam.io.WriteToText(r'C:\Users\Administrator\Documents\so\projectV1\output')
    
)

#Returns the oldest invoice of each customer
p1_oldestInvoice = (
    p0_firstStep
    #Aufpassen muss filter sein, sonst None Werte
    |beam.Filter(lambda element: element[4]>0)
    |beam.Filter(removeNotDue)
    
    |beam.Map(cleanTableDueDate)
    
    |'GroupBy' >> beam.GroupByKey()

    |beam.Map(calculateOldestInvoiceDays)

   
)

#Returns the % of achievement in creditlimit
    
p1_creditlimiteAchievement = (
    p0_firstStep
    #|beam.Map(lambda element: print(element))
    |beam.Map(lambda element: (element[0], element[4]))
    #|'GroupByCredi' >> beam.GroupByKey()
    |beam.CombinePerKey(sum)
    |beam.Map(lambda element: (element[0], round(element[1],2)))
    |beam.Map(totalInPercOfCreditlimit)
   # |beam.Map(lambda element: print(element))

)

p1_totalOpenInvoice = (
    p0_firstStep   
    |beam.Map(lambda element: (element[0], element[4]))
    |'combines the invoice amounts'>>beam.GroupByKey()

    |beam.Map(addUpAmountsInvoice)
    #|beam.Map(lambda e: print(e))
    #|beam.Map(lambda e: e[0], {"Total_Open": e[1]})
    
    
    # return element[0], {"Oldest_Invoice": difference.days}
    # |beam.Map(lambda element: print(element)) 
    # |beam.io.WriteToText(r'C:\Users\Administrator\Documents\so\projectV1\output')
    
)

p1_totalDueAmount = (
    p0_verfall   
    |beam.Map(lambda element: (element[0], element[5]))
    |'combines the due amounts'>>beam.GroupByKey()
    |'adds up due amounts' >> beam.Map(addUpAmountsDue)
   # |'adds up due amountks' >>beam.Map(lambda e: print(e))
    #|beam.Map(lambda e: e[0], {"Total_Open": e[1]})
    
    
    # return element[0], {"Oldest_Invoice": difference.days}
    # |beam.Map(lambda element: print(element)) 
    # |beam.io.WriteToText(r'C:\Users\Administrator\Documents\so\projectV1\output')
    
)



def formatFinalP4(element):

    KdNr = element[0] 
    uploadTime = datetime.today()
    

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
    
    for e in side_input_credit_limit_list:
        if e[0] == KdNr:
            credit_limit = e[1]
    if credit_limit == 0:
        credit_limit = -10
           # break
     
    
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
    
        
        #    'Upload_Datetime':uploadTime
    
   # print(len(element[1]))
   # print(element[1][0][0])

    
    #for i in element[1]:
    #    returnArray.append(i)
    
    #print (returnArray)
    #return (returnArray)
    
#| "ReadFromInMemory" >> beam.Create([{"kdNr": "546512","id": "546", "art": "kred", "val": "4.5"}])

p4_combined_results = (
    (p1_verfallInProzent, p1_oldestInvoice, p1_creditlimiteAchievement, p1_totalOpenInvoice, p1_totalDueAmount)
    |beam.CoGroupByKey()
    |beam.Map(formatFinalP4)
    |beam.Map(calculationBlockedFutureOrders)
    
 #   |beam.Map(lambda element: print(element))
    | "WriteToMySQL" >> write_to_mysql

)

p.run().wait_until_finish()
