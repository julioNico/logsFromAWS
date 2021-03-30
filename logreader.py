from __future__ import print_function

import boto3
import dateutil.parser
from datetime import timedelta, datetime
import datetime
import time
import json

print('Loading function')

def lambda_handler(event, context):
    time.sleep(300) 
    print("Received event: " + json.dumps(event, indent=2))
    Timestamp = event['Records'][0]['Sns']['Timestamp']
    print("From SNS: " + Timestamp)
    
    #Timestamp = "1970-01-01T21:12:33.000Z"
    print("Timestamp: " + Timestamp)
    FinishLog = dateutil.parser.parse(Timestamp)
    print(FinishLog)
    StartLog = (FinishLog - timedelta(minutes=15)) #.replace(microsecond=0)
    print("\n")
    print(StartLog)
    print(StartLog.time().replace(microsecond=0))
    print(StartLog.date())
    
    print("'" + str(StartLog.date()) + "-" + str(StartLog.time()) + "','yyyy-MM-dd-HH:mm:ss'")

    client = boto3.client('athena')
    
    response = client.start_query_execution(
        QueryString='''SELECT client_ip,
             request_url,
             target_status_code,
             elb_status_code
    FROM alb_logs
    WHERE parse_datetime(time,'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z')
        BETWEEN parse_datetime('''"'" + str(StartLog.date()) + "-" + str(StartLog.time().replace(microsecond=0)) + "','yyyy-MM-dd-HH:mm:ss')"'''
            AND parse_datetime('''"'" + str(FinishLog.date()) + "-" + str(FinishLog.time().replace(microsecond=0)) + "','yyyy-MM-dd-HH:mm:ss')"'',
        QueryExecutionContext={
            'Database': 'site_prd_alb',
            'Catalog': 'AwsDataCatalog'
        },
        ResultConfiguration={
            'OutputLocation': 's3://example-site-logs-hml/string',
        }
    )
    print("QueryExecutionId: " + response["QueryExecutionId"])
    
    QueryExecutionIdTeste = response["QueryExecutionId"]
    
    response = client.get_query_execution(
        QueryExecutionId= QueryExecutionIdTeste
    )
    
    while (response["QueryExecution"]["Status"]["State"]) != 'SUCCEEDED':
        response = client.get_query_execution( QueryExecutionId= QueryExecutionIdTeste )
        print(response["QueryExecution"]["Status"]["State"])
    
    data = client.get_query_results(
        QueryExecutionId= QueryExecutionIdTeste,
        MaxResults=123
    )
    
    print("Total de itens: " + str(len(data["ResultSet"]["Rows"])) + "\n \n")
    
    response_1 = (
            data["ResultSet"]["Rows"][0]["Data"][0]['VarCharValue'] + "\t" + "\t" +
            data["ResultSet"]["Rows"][0]["Data"][2]['VarCharValue'] + "\t" +
            data["ResultSet"]["Rows"][0]["Data"][3]['VarCharValue'] + "\t" + "\t" +
            data["ResultSet"]["Rows"][0]["Data"][1]['VarCharValue'] + "\n"
        )
    response_3 = response_1
    f = open("/tmp/logread.txt", 'w') #open(/tmp/file.txt)
    f.write(str(response_1))
    f.close()
    print(response_1)
    i = 1
    
    while i < len(data["ResultSet"]["Rows"]):
        response_2 = (
                data["ResultSet"]["Rows"][i]["Data"][0]['VarCharValue'] + "\t" + "\t" +
                data["ResultSet"]["Rows"][i]["Data"][2]['VarCharValue'] + "\t" + "\t" + "\t" +
                data["ResultSet"]["Rows"][i]["Data"][3]['VarCharValue'] + "\t" + "\t" + "\t" +
                data["ResultSet"]["Rows"][i]["Data"][1]['VarCharValue'] + "\n"
            )
        response_3 = response_3 + response_2
        f = open("/tmp/logread.txt", 'a')
        f.write(str(response_2))
        print(response_2)
        i = i + 1
    f.close()
    
    #Add day and hour from logs
    today = datetime.date.today()
    
    now = datetime.datetime.now()
    
    t = now.strftime("%H:%M:%S")
    resultado = str(today) + "_" + t + ".txt"
    
    #up logs to s3
    s3 = boto3.resource('s3')
    s3.Bucket('example-logreader').upload_file('/tmp/logread.txt', resultado, ExtraArgs={'ACL': 'public-read'})
    
    print("Upload finished!")
    
    print(response_3)
    
    client = boto3.client('ses')
    response = client.send_raw_email(
        Destinations=['mailDestination1@mail.com.br', 'mailDestination2@mail.com.br'],
        RawMessage={
            'Data': 'From: sourceName@example.org.br\nTo: mailDestination1@mail.com.br , mailDestination2@mail.com.br\nSubject: Test email (contains an attachment)\nMIME-Version: 1.0\nContent-type: Multipart/Mixed; boundary="NextPart"\n\n--NextPart\nContent-Type: text/plain\n\nLog de alerta.\n\n--NextPart\nContent-Type: text/plain;\nContent-Disposition: attachment; filename="'+ resultado +'"\n\n' + response_3 + '\n\n--NextPart--',
        },
        Source='logreader@example.org.br'
    )
    print(response)
        
    return Timestamp