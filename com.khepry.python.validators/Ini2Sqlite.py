# -*- coding: utf-8 -*-

import codecs
import collections
import configparser
import csv
import os
import sqlite3
import sys
import time
import warnings


# ignore all warnings so
# as to not confuse the user
warnings.filterwarnings("ignore")

maxRecords = 0
flushCount = 10000

createTables = True
populateWithData = True

colDelimiter = ','

csvPath = "/home/projects/data/Voters/NC"
csvFile = "ncvoter92.csv"

iniPath = "/home/projects/temp/NC"
iniFile = "ncvoter92.ini"

tgtPath = "/home/projects/temp/NC"
tgtFile = "ncvoter92.sqlite"
tgtName = "ncvoters"

dftDataType = "VARCHAR"
dftDataSize = "255"

databaseType = "SQLITE"

def main():

    print ("Main routine")
    
    # derive the CSV, INI, and TGT file paths    
    csvFilePath = os.path.join(csvPath, csvFile)
    iniFilePath = os.path.join(iniPath, iniFile)
    tgtFilePath = os.path.join(tgtPath, tgtFile)

    # obtain any command-line arguments
    # overriding any pre-existing values
    nextArg = ""
    for argv in sys.argv:
        
        if nextArg != "":
            if nextArg == "csvFilePath":
                csvFilePath = argv
            elif nextArg == "iniFilePath":
                iniFilePath = argv
            elif nextArg == "tgtFilePath":
                tgtFilePath = argv
            nextArg = ""
        else:
            if argv.lower() == "--csvFilePath":
                nextArg = "csvFilePath"
            elif argv.lower() == "--iniFilePath":
                nextArg = "iniFilePath"
            elif argv.lower() == "--tgtFilePath":
                nextArg = "tgtFilePath"

    # expand the CSV, INI, and TGT file paths
    csvFilePathExpanded = os.path.expanduser(csvFilePath)
    iniFilePathExpanded = os.path.expanduser(iniFilePath)
    tgtFilePathExpanded = os.path.expanduser(tgtFilePath)
    
    print ("Verifying TGT folder path '%s'" % os.path.dirname(tgtFilePathExpanded))
    if os.path.dirname(tgtFilePathExpanded) != "":
        if not os.path.exists(os.path.dirname(tgtFilePathExpanded)):
            os.makedirs(os.path.dirname(tgtFilePathExpanded))
            
    
    # establish a connection to the database
    # and instantiate a SQL cursor object    
    conSQLite = sqlite3.connect(tgtFilePathExpanded)
    sqlCursor = conSQLite.cursor()

    # initialize working variable(s)    
    txt = []
 
    if createTables:

        print ("Read the INI file")
        print ("")
        
        # obtain the settings
        # from the INI file path
        config = configparser.ConfigParser()
        config.optionxform = str #this will preserve the case of the section names
        config._interpolation = configparser.ExtendedInterpolation()
        config.read(iniFilePathExpanded)
        
        derivedColumns = collections.OrderedDict(config.items("derived"))
        
        nbrOfColumns = len(derivedColumns)
    
        print ("Verifying INI file path '%s'" % iniFilePathExpanded)    
        if not os.path.exists(iniFilePathExpanded):
            sys.stderr.write("ERROR: INI file path does NOT exist '%s'" % iniFilePath)
            sys.stderr.write("\n")
            return
        
        # derive the CREATE TABLE statement
        # for the table that will hold specs
        txt.clear()
        txt.append("BEGIN TRANSACTION;\n")
        txt.append("DROP TABLE IF EXISTS [%s%s];\n" % (tgtName, "_specs"))
        txt.append("CREATE TABLE [%s%s] (\n" % (tgtName, "_specs"))    
        txt.append("\t[colName] VARCHAR(50) DEFAULT NULL,\n")
        txt.append("\t[dataType] VARCHAR(50) DEFAULT NULL,\n")
        txt.append("\t[minSize] INT DEFAULT NULL,\n")
        txt.append("\t[maxSize] INT DEFAULT NULL,\n")
        txt.append("\t[avgSize] INT DEFAULT NULL,\n")
        txt.append("\t[cvgPct] INT DEFAULT NULL,\n")
        txt.append("\t[lookUp] VARCHAR(50) DEFAULT NULL\n")
        txt.append(");\n")
        txt.append("END TRANSACTION;\n")
    
        sql = ''.join(txt)
        print (sql)
       
        # create the table
        sqlCursor.executescript(sql)
        
        print ("")
        print ("Committing changes to database")
    
        # commit the changes
        conSQLite.commit()
    
        # derive the INSERT INTO statements
        # for the table that will hold specs
    
        txt.clear()
        txt.append("BEGIN TRANSACTION;\n")
        for key, value in derivedColumns.items():
            pieces = value.split(',')
            dataType = pieces[0]
            minSize = pieces[1]
            maxSize = pieces[2]
            avgSize = pieces[3]
            cvgPct = pieces[4]
            txt.append("INSERT INTO [%s%s] VALUES('%s','%s',%s,%s,%s,%s,'%s');\n" % (tgtName, "_specs", key, dataType, minSize, maxSize, avgSize, cvgPct, key))
        txt.append("END TRANSACTION;\n")
    
        sql = ''.join(txt)
        print (sql)
       
        # create the table
        sqlCursor.executescript(sql)
        
        print ("")
        print ("Committing changes to database")
    
        # commit the changes
        conSQLite.commit()
        
        # derive the CREATE TABLE statement
        # for the table that will hold codes
        
        txt.clear()
        txt.append("BEGIN TRANSACTION;\n")
        txt.append("DROP TABLE IF EXISTS [%s%s];\n" % (tgtName, "_codes"))
        txt.append("CREATE TABLE [%s%s] (\n" % (tgtName, "_codes"))    
        txt.append("\t[colName] VARCHAR(50) DEFAULT NULL,\n")
        txt.append("\t[srcValue] VARCHAR(255) DEFAULT NULL,\n")
        txt.append("\t[tgtValue] VARCHAR(255) DEFAULT NULL,\n")
        txt.append("\t[valCount] INT DEFAULT 0,\n")
        txt.append("\t[pctTotal] DOUBLE DEFAULT 0")
        txt.append(");\n")
        txt.append("END TRANSACTION;\n")
    
        sql = ''.join(txt)
        print (sql)
       
        # create the table
        sqlCursor.executescript(sql)
        
        print ("")
        print ("Committing changes to database")
    
        # commit the changes
        conSQLite.commit()
    
        # derive the INSERT INTO statements
        # for the table that will hold codes
    
        txt.clear()
        txt.append("BEGIN TRANSACTION;\n")
        codes = collections.OrderedDict()
        for key in derivedColumns.keys():
            codes.clear()
            codes = collections.OrderedDict(config.items('values.%s' % key.lower()))
            for code, value in codes.items():
                pieces = value.split(',')
                valCount = pieces[0]
                pctTotal = pieces[1]
                # print (key, code, pctOfTotal)
                txt.append("INSERT INTO [%s%s] VALUES('%s','%s','%s',%d,%f);\n" % (tgtName, "_codes", key, code.replace("'", "''"), code.replace("'", "''"), int(valCount), float(pctTotal)))    
        txt.append("END TRANSACTION;\n")
    
        sql = ''.join(txt)
        print (sql)
       
        # create the table
        sqlCursor.executescript(sql)
        
        print ("")
        print ("Committing changes to database")
        
        # commit the changes
        conSQLite.commit()
        
        # derive the CREATE TABLE statement
        # for the table that will hold data
    
        columns = 0
        txt.clear()
        txt.append("BEGIN TRANSACTION;\n")
        txt.append("DROP TABLE IF EXISTS [%s];" % tgtName)
        txt.append("CREATE TABLE [%s] (\n" % tgtName)
        for key, value in derivedColumns.items():
            columns += 1        
            # print (key, value)
            pieces = value.split(',')
            dataType = pieces[0]
            dataSize = pieces[2]
            if columns < nbrOfColumns:
                txt.append("\t[%s] %s(%s) DEFAULT NULL,\n" % (key, dataType, dataSize))
            else:
                txt.append("\t[%s] %s(%s) DEFAULT NULL\n" % (key, dataType, dataSize))
        txt.append(");\n")
        txt.append("END TRANSACTION;\n")
    
        sql = ''.join(txt)
        print (sql)
       
        # create the table
        sqlCursor.executescript(sql)
        
        print ("")
        print ("Committing changes to database")
    
        # commit the changes
        conSQLite.commit()
    
    if populateWithData:
    
        print ("Verifying CSV file path '%s'" % csvFilePathExpanded)    
        if not os.path.exists(csvFilePathExpanded):
            sys.stderr.write("ERROR: CSV file path does NOT exist '%s'" % csvFilePath)
            sys.stderr.write("\n")
            return
            
        fReader = codecs.open(csvFilePathExpanded, 'r', 'cp1252')    
        csvreader=csv.reader(fReader, delimiter=colDelimiter)
        
        # establish a connection to the database
        # and instantiate a SQL cursor object    
        conSQLite = sqlite3.connect(tgtFilePathExpanded)
        sqlCursor = conSQLite.cursor()
        
        bgnTime = time.time()

        txt.clear()
        
        val = []
        rows = 0
        for row in csvreader:
            # print (rows)
            # if header row
            if rows == 0:
                rows += 1
                if databaseType == "SQLITE":
                    sqlCursor.execute("DELETE FROM [%s]" % tgtName)
                else:
                    sqlCursor.execute("TRUNCATE TABLE [%s]" % tgtName)
                conSQLite.commit()
                continue
                # bypass this branch's remaining code for the time being
                txt.append("CREATE TABLE [%s]\n(\n" % tgtName)
                for i in range(0, len(row)):
                    txt.append("    %s %s(%s) DEFAULT NULL,\n" % (row[i].upper(), dftDataType, dftDataSize))
                tblCmd = ''.join(txt)
                tblCmd = tblCmd[:-2]
                tblCmd += "\n"
                tblCmd += ");"
                txt.clear()
                # output tblCmd to console
                print ()    
                print ("tblCmd:")
                print (tblCmd)
                # create the table in the database
                drpCmd = 'DROP TABLE IF EXISTS [%s];' % tgtName
                sqlCursor.execute(drpCmd)    
                sqlCursor.execute(tblCmd)
            # else data row
            else:
                val.clear()
                for i in range(0, len(row)):
                    val.append("'%s'," % row[i].replace("'","''"))
                values = ''.join(val)[:-1]
                if len(txt) == 0:
                    txt.append("BEGIN TRANSACTION;\n")
                txt.append("INSERT INTO [%s] VALUES(%s);\n" % (tgtName, values))
                rows += 1
                if rows % flushCount == 0:
                    txt.append("END TRANSACTION;\n")
                    sqlCmd = ''.join(txt)
                    # print (sqlCmd)
                    sqlCursor.executescript(sqlCmd)
                    conSQLite.commit()
                    txt.clear()
                    endTime = time.time()
                    elapsedTime = endTime - bgnTime
                    if elapsedTime == 0:
                        elapsedTime = 1
                    rcdsPerSec = rows / elapsedTime
                    print ("Rows: {:,} @ {:,.0f} records/second".format(rows, rcdsPerSec))
                if maxRecords > 0 and rows >= maxRecords:
                    break
        
        #clean up
        if len(txt) > 0:
            txt.append("END TRANSACTION;\n")
            sqlCmd = ''.join(txt)
            # print (sqlCmd)
            sqlCursor.executescript(sqlCmd)
        conSQLite.commit()
        sqlCmd = ""
        fReader.close()
    
        print ("-----------------------------------")
        endTime = time.time()
        elapsedTime = endTime - bgnTime
        if elapsedTime == 0:
            elapsedTime = 1
        rows -= 1
        rcdsPerSec = rows / elapsedTime
        print ("Rows: {:,} @ {:,.0f} records/second".format(rows, rcdsPerSec))
        print ("")

    # close the
    # database connection
    conSQLite.close()
    
    print ("")    
    print ("Processing Ended")
    
    return
    
        
# -------------------------------------------------------------------------
# execute the "main" method
# -------------------------------------------------------------------------

if __name__ == "__main__":
    main()
    