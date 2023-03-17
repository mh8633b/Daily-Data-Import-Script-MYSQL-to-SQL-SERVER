import pyodbc
import os
import sys
import traceback
import pymysql
import logging
import requests
import threading
import time
import pdb
import schedule
import configparser
import datetime

try:
    config = configparser.RawConfigParser()
    config.read("config.ini", encoding='utf-8-sig')
    HOST = config.get('PARAMETER', 'HOST')
    DB = config.get('PARAMETER', 'DB')
    HOUR = int(config.get('PARAMETER', 'HOUR'))
    if HOUR not in range(0, 25):
        print('Error in config file for HOUR param.')
        raise ZeroDivisionError
    HOUR = str(HOUR).zfill(2)

    MINUTE = int(config.get('PARAMETER', 'MINUTE'))
    if MINUTE not in range(0, 61):
        print('Error in config file for MINUTE param.')
        raise ZeroDivisionError
    MINUTE = str(MINUTE).zfill(2)
except:
    print("Error in config file. Please review it run the tool again.")
    os.system('pause')
    raise SystemExit


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler('running.log')

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def read_data_from_mysql():
    conn = pymysql.connect(db=DB, host=HOST, user='hrms', password='')
    cursor = conn.cursor()
    cursor.execute('select * from employeemanagement$employees')
    data = list()
    try:
        for result in cursor:
            temp_dict = dict()
            temp_dict['id'] = result[0]
            temp_dict['processedbyname'] = result[1]
            temp_dict['currentstatus'] = result[2]
            temp_dict['product'] = result[3]

            if result[4]:
                temp_dict['grade'] = result[4]
            else:
                temp_dict['grade'] = 0

            temp_dict['level'] = result[5]

            if result[6]:
                temp_dict['lastworkday'] = "convert(datetime, '%s')" % result[6]
            else:
                temp_dict['lastworkday'] = "convert(datetime, '')"

            temp_dict['processedbyid'] = result[7]
            temp_dict['employeeid'] = result[8]
            temp_dict['employeename'] = result[9]
            temp_dict['pendingstatus'] = result[10]

            if result[11]:
                temp_dict['joindate'] = "convert(datetime, '%s')" % result[11]
            else:
                temp_dict['joindate'] = "convert(datetime, '')"

            temp_dict['employeetype'] = result[12]

            if result[13] == '':
                temp_dict['employeeoldid'] = None
            else:
                temp_dict['employeeoldid'] = result[13]

            temp_dict['employeeoldtype'] = result[14]
            temp_dict['nationality'] = result[15]
            temp_dict['attachment'] = result[16]
            temp_dict['requestedupdate'] = result[17]
            temp_dict['project'] = result[18]
            temp_dict['supplier'] = result[19]
            if result[20]:
                temp_dict['contractexpirydate'] = "convert(datetime, '%s')" % result[20]
            else:
                temp_dict['contractexpirydate'] = "convert(datetime, '')"

            temp_dict['transferstatus'] = result[21]

            if result[22]:
                temp_dict['changeddate'] = "convert(datetime, '%s')" % result[22]
            else:

                temp_dict['changeddate'] = "convert(datetime, '')"

            if result[23]:
                temp_dict['createddate'] = "convert(datetime, '%s')" % result[23]
            else:
                temp_dict['createddate'] = "convert(datetime, '')"

            if result[24]:
                temp_dict['transferdate'] = "convert(datetime, '%s')" % result[24]
            else:
                temp_dict['transferdate'] = "convert(datetime, '')"

            temp_dict['undertransfer'] = result[25]
            temp_dict['internaltransfertype'] = result[26]
            temp_dict['contactno'] = result[27]

            if result[28]:
                temp_dict['dateofbirth'] = "convert(datetime, '%s')" % result[28]
            else:
                temp_dict['dateofbirth'] = "convert(datetime, '')"

            temp_dict['cnic'] = result[29]
            temp_dict['emergencycontactno'] = result[30]
            temp_dict['scannedcnic'] = result[31]
            temp_dict['clearanceform'] = result[32]
            temp_dict['image'] = result[32]
            temp_dict['certificationsstatus'] = result[33]
            data.append(temp_dict)
            del temp_dict
    except:
        logger.error('Error while reading from MySQL.')
        logger.info(sys.exc_info()[1])
        logger.info(traceback.format_exc())
    conn.close()
    return data


def push_data_into_sql_server(data):
    conn = pyodbc.connect("DRIVER={SQL Server}; SERVER=''; DATABASE=''; Trusted_Connection=yes;")
    cursor = conn.cursor()
    cursor.execute('delete from employeedetailsall')
    count = 0
    try:
        for temp_dict in data:
            try:
                    sql_query = "INSERT INTO employeedetailsall(id,processedbyname,currentstatus,product,grade,levels,lastworkday," \
                                "processbyid,employeeid,employeename,pendingstatus,joindate,employeetype,employeeoldid,nationality,requestedupdate," \
                                "project,supplier,contractexpirydate,transferstatus,changeddate,createddate,transferdate,undertransfer," \
                                "internaltransfertype,contactno,dateofbirth,cnic,emergencycontactno,certificationstatus) " \
                                "VALUES({0},'{1}','{2}','{3}',{4},'{5}',{6},'{7}','{8}','{9}','{10}',{11},'{12}','{13}','{14}',{15},'{16}','{17}'," \
                                "{18},'{19}',{20},{21},{22},'{23}','{24}','{25}',{26},'{27}','{28}','{29}');".format(
                        temp_dict['id'], temp_dict['processedbyname'], temp_dict['currentstatus'], temp_dict['product'], temp_dict['grade'],
                        temp_dict['level'], temp_dict['lastworkday'], temp_dict['processedbyid'], temp_dict['employeeid'], temp_dict['employeename'],
                        temp_dict['pendingstatus'], temp_dict['joindate'], temp_dict['employeetype'], temp_dict['employeeoldid'],
                        temp_dict['nationality'], temp_dict['requestedupdate'], temp_dict['project'], temp_dict['supplier'],
                        temp_dict['contractexpirydate'], temp_dict['transferstatus'], temp_dict['changeddate'], temp_dict['createddate'],
                        temp_dict['transferdate'], temp_dict['undertransfer'], temp_dict['internaltransfertype'], temp_dict['contactno'],
                        temp_dict['dateofbirth'], temp_dict['cnic'], temp_dict['emergencycontactno'], temp_dict['certificationsstatus']);
                    cursor.execute(sql_query)
                    # pdb.set_trace()
                    # cursor.execute('SELECT * FROM employeedetailsall').fetchall()
                    count += 1
            except:
                    cursor.execute('ALTER TABLE employeedetailsall DROP COLUMN id')
                    cursor.execute('ALTER TABLE employeedetailsall ADD id bigint')
                    try:
                        cursor.execute(sql_query)
                        sql_query = "INSERT INTO employeedetailsall(id,processedbyname,currentstatus,product,grade,levels," \
                                    "lastworkday,processbyid,employeeid,employeename,pendingstatus,joindate,employeetype,employeeoldid,nationality," \
                                    "requestedupdate,project,supplier,contractexpirydate,transferstatus,changeddate,createddate,transferdate," \
                                    "undertransfer,internaltransfertype,contactno,dateofbirth,cnic,emergencycontactno,certificationstatus) " \
                                    "VALUES({0},'{1}','{2}','{3}',{4},'{5}',{6},'{7}','{8}','{9}','{10}',{11},'{12}','{13}','{14}',{15},'{16}'," \
                                    "'{17}',{18},'{19}',{20},{21},{22},'{23}','{24}','{25}',{26},'{27}','{28}','{29}');".format(
                            temp_dict['id'], temp_dict['processedbyname'], temp_dict['currentstatus'], temp_dict['product'], temp_dict['grade'],
                            temp_dict['level'], temp_dict['lastworkday'], temp_dict['processedbyid'], temp_dict['employeeid'],
                            temp_dict['employeename'], temp_dict['pendingstatus'], temp_dict['joindate'], temp_dict['employeetype'],
                            temp_dict['employeeoldid'], temp_dict['nationality'], temp_dict['requestedupdate'], temp_dict['project'],
                            temp_dict['supplier'], temp_dict['contractexpirydate'], temp_dict['transferstatus'], temp_dict['changeddate'],
                            temp_dict['createddate'], temp_dict['transferdate'], temp_dict['undertransfer'], temp_dict['internaltransfertype'],
                            temp_dict['contactno'], temp_dict['dateofbirth'], temp_dict['cnic'], temp_dict['emergencycontactno'],
                            temp_dict['certificationsstatus']);
                        count += 1
                    except:
                        temp_dict['dateofbirth'] = "convert(datetime, '')"
                        sql_query = "INSERT INTO employeedetailsall(id,processedbyname,currentstatus,product,grade,levels," \
                                    "lastworkday,processbyid,employeeid,employeename,pendingstatus,joindate,employeetype,employeeoldid," \
                                    "nationality,requestedupdate,project,supplier,contractexpirydate,transferstatus,changeddate,createddate," \
                                    "transferdate,undertransfer,internaltransfertype,contactno,dateofbirth,cnic,emergencycontactno," \
                                    "certificationstatus) VALUES({0},'{1}','{2}','{3}',{4},'{5}',{6},'{7}','{8}','{9}','{10}',{11},'{12}','{13}'," \
                                    "'{14}',{15},'{16}','{17}',{18},'{19}',{20},{21},{22},'{23}','{24}','{25}',{26},'{27}','{28}','{29}');".format(
                            temp_dict['id'], temp_dict['processedbyname'], temp_dict['currentstatus'], temp_dict['product'], temp_dict['grade'],
                            temp_dict['level'], temp_dict['lastworkday'], temp_dict['processedbyid'], temp_dict['employeeid'],
                            temp_dict['employeename'], temp_dict['pendingstatus'], temp_dict['joindate'], temp_dict['employeetype'],
                            temp_dict['employeeoldid'], temp_dict['nationality'], temp_dict['requestedupdate'], temp_dict['project'],
                            temp_dict['supplier'], temp_dict['contractexpirydate'], temp_dict['transferstatus'], temp_dict['changeddate'],
                            temp_dict['createddate'], temp_dict['transferdate'], temp_dict['undertransfer'], temp_dict['internaltransfertype'],
                            temp_dict['contactno'], temp_dict['dateofbirth'], temp_dict['cnic'], temp_dict['emergencycontactno'],
                            temp_dict['certificationsstatus']);
                        cursor.execute(sql_query)
                        count += 1
    except:
        logger.error('Exception in writing data')
        logger.info(sql_query)
        logger.info(sys.exc_info()[1])
        logger.info(traceback.format_exc())

    conn.commit()
    conn.close()
    return count


def post_itms(tool_id, status, units, sender, request_type, project, api_url='api_url'):
    """
    send stats to iTMS Platform for specific Tool
        tool_id:  (int) get it from iTMS System for the specific tool
        project='': (string) Project Code or Project Name for the request
        status='Success': (String) accepted options are: Success, Failure, Error
        units=0: (int) number of DU or any other measurement unit for every request size
        sender='': (string) sender of request
        execution_time=None: execution time in format: "2018-12-19T13:51:42.211Z". Default is current time
        api_url='api_url': api_url
    """
    import datetime
    request_body = dict()
    request_body['Project'] = project
    request_body['Status'] = status
    request_body['Units'] = units
    request_body['Sender'] = sender
    request_body['Tool'] = {'ToolID': tool_id}
    request_body['RequestType'] = request_type
    request_body['ExecutionDate'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    try:
        r = requests.post(api_url, json=request_body)
        logger.info('Stats sent {0}'.format(['Success', 'Error'][r.status_code != 200]))
    except Exception as e:
        logger.info('Some error occurred during sending stats')


def create_thread(function_name, *args, **kwargs):
    try:
        thread = threading.Thread(target=function_name, args=args, kwargs=kwargs)
        thread.start()
        logger.info("Thread started...")
    except:
        logger.info('Error in Thread creation. %s' % function_name)


def function():
    date = datetime.datetime.now()
    print(('PROCESS STARTED at %s ' % (date.strftime('%d-%b-%Y %H:%M'))).center(50, '-') + '\n')
    logger.info('Reading...')
    data = read_data_from_mysql()
    print('%s rows fetched.' % len(data))
    logger.info('%s rows fetched.' % len(data))
    count = push_data_into_sql_server(data)
    print("%s rows inserted." % count)
    logger.info("%s rows inserted." % count)
    del data
    print('...Program Ended')
    logger.info('Program Ended')

    logger.info('Sending Stats')
    if count:
        create_thread(post_itms, tool_id=1375, status='Success', units=1, sender=os.getlogin(), request_type="deployed", project="")
        print('Success\n\n')
        logger.info('Success\n')
    else:
        create_thread(post_itms, tool_id=1375, status='Failed', units=1, sender=os.getlogin(), request_type="deployed", project="")
        print('Failed\n\n')
        logger.info('Failed\n')


function()
schedule.every().day.at("%s:%s" % (HOUR, MINUTE)).do(function)
print('Program Started...')
while True:
    schedule.run_pending()
    time.sleep(30)
