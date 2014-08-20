# -*- coding: utf-8 -*-

import codecs
import collections
import csv
import operator
import os
import sqlite3
import sys
import time

maxRecords = 0
flushCount = 10000

minValues = 3
maxValues = 100
minSampleSize = 10000

dftDataType = "VARCHAR"
dftDataSize = "255"

mainPath = "/home/projects/data/Voters"
srcPath = "NC"
tempPath = "/home/projects/temp"
tgtPath = "NC"
iniFile = "NcVoterCsvSpecs.ini"
    
csvs = {
    'ncvoter48':['ncvoter48.csv', ','],
#    'ncvoter92':['ncvoter92.csv', ','],
#    'NC_Voters_StateWide':['NC_Voters_StateWide.csv', ','],
    '':['','']
}

# TODO: Create an INI file and obtain values from it
# TODO: Read the resulting INI file and render it appropriately as a SQLite database file.
# TODO: Render this code as a multi-threaded or multi-processed solution
# TODO: Discern likely data type of each column (a nicety, not a necessity) 

def main():
    
    sourcePath = os.path.abspath(os.path.expanduser(os.path.join(mainPath, srcPath)))
    targetPath = os.path.abspath(os.path.expanduser(os.path.join(tempPath, tgtPath)))
    
    append2Ini = False
    for key,csvInfo in csvs.items():
        if key != "":
            csvFile = csvInfo[0].strip()
            colDelimiter = csvInfo[1]
            csvFileName = os.path.join(sourcePath, csvFile)
            iniFileName = os.path.join(targetPath, key + ".ini")        
            csv2iniFile(csvFileName, iniFileName, key, colDelimiter, dftDataType, dftDataSize, maxRecords, flushCount, append2Ini)
        
    return
    
        
# -------------------------------------------------------------------------
# define the Look-Up Table CSV to SQLite file method
# -------------------------------------------------------------------------
def csv2iniFile(csvFileName, iniFileName, tblName, colDelimiter, dftDataType, dftDataSize, maxRecords, flushCount, append=False):
    
    print("")
    print("==============================================")
    print("CSV to INI file conversion...")
    print("----------------------------------------------")
    print("csvFileName '%s'" % csvFileName)
    print("iniFileName '%s'" % iniFileName)
    
    # expand any leading tilde
    # to the user's home path
    csvFileName = os.path.expanduser(csvFileName)
    iniFileName = os.path.expanduser(iniFileName)
     
    # verify that CSV file exists
    if not os.path.exists(csvFileName):
        print ("CSV file '%s' does NOT exist!" % csvFileName)
        return
        
    # make sure the target folder exists,
    # creating it recursively if it does not
    if not os.path.exists(os.path.dirname(iniFileName)):
        os.makedirs(os.path.dirname(iniFileName))
    
    # open file with Windows-1252 encoding    
    fReader = codecs.open(csvFileName, 'r', 'cp1252')    
    csvreader=csv.reader(fReader, delimiter=colDelimiter)
    
    if not append:
        fWriter = open(iniFileName, 'w')
    else:
        fWriter = open(iniFileName, 'a')
    
    bgnTime = time.time()

    colNames = []
    
    avgLengths = collections.OrderedDict()    
    maxLengths = collections.OrderedDict()
    minLengths = collections.OrderedDict()
    totLengths = collections.OrderedDict()
    totNonBlanks = collections.OrderedDict()
    cvgPercent = collections.OrderedDict()
    
    colValues = collections.OrderedDict()
    
    rows = 0
    # row-by-row
    for row in csvreader:
        # increment row counter
        rows += 1
        # if header row
        if rows == 1:
            # column-by-column
            for i in range(0, len(row)):
                # initialize column names collection
                colNames.append(row[i])
            # initialize the stats collections
            for colName in colNames:
                try:
                    # Python 2
                    minLengths[colName] = sys.maxint
                    maxLengths[colName] = -sys.maxint - 1
                except AttributeError:
                    # Python 3
                    minLengths[colName] = sys.maxsize
                    maxLengths[colName] = -sys.maxsize - 1
                totLengths[colName] = 0.0
                totNonBlanks[colName] = 0
                colValues[colName] = {}
        # else data row
        else:
            # column-by-columnm
            for i in range(0, len(row)):
                # obtain column name
                colName = colNames[i]
                # calculate the stats
                value = str(row[i].strip())
                # total the value's length
                # for later use in averaging
                totLengths[colName] += len(value)
                # compare and set the min and max lengths
                if len(value) < minLengths[colName]:
                    minLengths[colName] = len(value)
                if len(value) > maxLengths[colName]:
                    maxLengths[colName] = len(value)
                # if value is non-blank
                if value != "":
                    # increment non-blank count
                    totNonBlanks[colName] += 1
                    try:
                        # increment the value's count
                        # if already in the collection
                        colValues[colName][value] += 1
                    except:
                        # or initialize its count to 1
                        # if not yet in the collection
                        colValues[colName][value] = 1
                
        # flush output, if any, and output status                    
        if rows % flushCount == 0:
            endTime = time.time()
            elapsedTime = endTime - bgnTime
            if elapsedTime == 0:
                elapsedTime = 1
            rcdsPerSec = rows / elapsedTime
            print ("Rows: {:,} @ {:,.0f} records/second @ {:,.0f} seconds".format(rows, rcdsPerSec, elapsedTime))
            
        # if maxRecords specified
        # and rows exceeds maxRows
        if maxRecords > 0 and rows > maxRecords:
            # cease further processing
            break
                            
    # if the file had no rows
    # or had just a header row
    if rows <= 1:
        # column-by-column
        for colName in colNames:
            # zero the various collections
            avgLengths[colName] = 0
            minLengths[colName] = 0
            maxLengths[colName] = 0
            cvgPercent[colName] = 0
    else:
        # column-by-column
        for colName in colNames:
            # if maxLength was zero
            if maxLengths[colName] == 0:
                # default it to the default data size
                maxLengths[colName] = int(dftDataSize)
            # calculate the average length
            avgLengths[colName] = totLengths[colName] / (rows - 1.0)
            # calculate the coverage percent
            cvgPercent[colName] = totNonBlanks[colName] / (rows - 1.0)

    # output the 'derived' values
    # to the specified INI file
    fWriter.write("[%s]\n\n" % "derived")
    fWriter.write("\t;colName=dataType,minSize,maxSize,avgSize,cvgPct\n")
    for colName in colNames:
        fWriter.write("\t%s=%s,%d,%d,%d,%d\n" % (colName.upper(), dftDataType, minLengths[colName], maxLengths[colName], round(avgLengths[colName]), round(cvgPercent[colName] * 100)))
    fWriter.write("\n")

    colValuesSorted = collections.OrderedDict()
    # column-by-column
    for colName in colNames:
        # sort the column values by descending order to occurrence
        try:
            # Python 2
            colValuesSorted[colName] = sorted(colValues[colName].iteritems(), key=operator.itemgetter(1), reverse=True)
        except AttributeError:
            # Python 3
            colValuesSorted[colName] = sorted(colValues[colName].items(), key=operator.itemgetter(1), reverse=True)
    
    # column-by-column
    for colName in colNames:
        # output the desired number of values
        fWriter.write("[values.%s]\n\n" % colName)
        fWriter.write("\t;value=count,pctOfTotal\n")
        for key,value in colValuesSorted[colName][:maxValues]:
            # if the key is not blank
            # and enough rows were sampled
            # and the count of the first reverse-sorted value is greater than 1
            if key != "" and rows >= maxValues and colValuesSorted[colName][0][1] >= minValues:
                # assume that the value is a code
                # and write out it's value and count
                try:
                    pctOfTotal = (value * 100.00) / totNonBlanks[colName]
                except:
                    pctOfTotal = 0
                
                fWriter.write("\t{}={:},{:.2f}\n".format(key.replace("=","\="), value, pctOfTotal))
        fWriter.write("\n")
           
    #clean up
    fWriter.close()
    fReader.close()

    print ("-----------------------------------")
    endTime = time.time()
    elapsedTime = endTime - bgnTime
    if elapsedTime == 0:
        elapsedTime = 1
    rcdsPerSec = rows / elapsedTime
    print ("Rows: {:,} @ {:,.0f} records/second @ {:,.0f} seconds".format(rows, rcdsPerSec, elapsedTime))
    print ("")
    return

        
# -------------------------------------------------------------------------
# execute the "main" method
# -------------------------------------------------------------------------

if __name__ == "__main__":
    main()
