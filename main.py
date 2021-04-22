from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
import pandas as pd
import math
import winsound


try:
    bulk = SalesforceBulk(username= username, password= password, security_token= token, sandbox= True)

    data = pd.read_csv('sampleData.txt',sep=',')

    data = data['Id']

    job = bulk.create_delete_job("exception_log__c", contentType='CSV')

    totalSize = data.size
    print('Total number of loops: '+ str(math.ceil(totalSize/10000)))

    batch = ''

    for i in range(math.ceil(totalSize/10000)):
        start = 10000*i
        end = 10000*(i+1) if 10000*(i+1)<totalSize else totalSize
        temp = data.loc[start:end-1]
        print(i+1)
        exceptionLogs = [dict(Id = salesforceId) for salesforceId in temp]
        csv_iter = CsvDictsAdapter(iter(exceptionLogs))
        batch = bulk.post_batch(job, csv_iter)
        
    bulk.wait_for_batch(job, batch)
    bulk.close_job(job)
    print("Done. exceptionLogs deleted.")
except:
    print('EXCEPTION OCCURED!!!')
finally:
    winsound.PlaySound("SystemExit", winsound.SND_ALIAS)