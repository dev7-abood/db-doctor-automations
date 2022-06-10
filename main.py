import pyodbc
import sqlite3
from dateutil.parser import parse
import datetime

msConn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\Abood\Desktop\doctor mahomad\edit.mdb;')
ms = msConn.cursor()

sqliteConn = sqlite3.connect('cuspBackupData.db')
sqlite = sqliteConn.cursor()

def insertDataToSqllite(table, parameters, values, count = 1):
    val = len(parameters) * ', ?'
    anKnowValues = ('(' + val[2:] + ')')
    query = f"INSERT INTO {table} {parameters} VALUES {anKnowValues};"
    sqlite.execute(query, values)
    sqliteConn.commit()
    print("Total", count, "Records inserted successfully into SqliteDb_developers table")


def gender(genderType):
    switcher = {
        'ذكر': '1',
        'Male': '1',
        'أنثى': '2',
        'انثى': '2',
        'Female': '2',
    }
    return switcher.get(genderType, None)

def filterValueLen(value, lenV=255):
    if len(str(value)) >= lenV:
        return None
    return value


# select data from some table

patients = ms.execute("SELECT * FROM Patients")
count = 0
for i in range(1, 4298 + 1):
    try:
        next = patients.fetchone()
    except:
        continue
    parameters = ('_name', '_email', '_phone1', '_phone2', '_address', '_insurance_number', '_gender', '_registration_date','_birthdate')

    fName, mName, lName = next[1] or '', next[2] or '', next[3] or ''
    name = fName + ' ' + mName + ' ' + lName

    try:
        birthOfDate = parse(str(next[7])).strftime("%d %B %Y").upper()
    except:
        birthOfDate = None

    values = (
        name or " ",  # name
        filterValueLen(next[6]),  # email
        filterValueLen(next[5]),  # phone1
        None,  # phone2,
        filterValueLen(next[4]),  # address
        filterValueLen(next[10]),  # insurance_number
        gender(next[12]),
        None,  # _registration_date
        birthOfDate #birth Of Date
    )
    count = count + 1
    insertDataToSqllite('general', parameters, values, count)

# # get all table names
# # index = -1
# # for row in ms.tables():
# #     index+=1
# #     print(row.table_name, '======>'+str(index))
# #
# #
# # index = -1
# # for row in ms.columns(table='Patients1'):
# #     index += 1
# #     print(row.column_name, '------> '+str(index))
#
#
# # try:
# #     for i in patients:
# #         print(i)
# # except Exception as varname:
# #     print(varname)
# #
ms.close()
sqlite.close()
