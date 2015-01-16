__author__ = 'Shyam Pinnipati, Ngyuen Ngo'

import psycopg2
import csv
import copy
import os

floatlist = ['WTPERFIN', 'SFWGT', 'GCDWORK', 'DISTTOSC', 'TDCASEID', 'TRPMILES', 'WTTRDFIN', 'VMT_MILE', 'GASPRICE'
            , 'WTHHFIN', 'ANNMILES', 'VEHOWNMO', 'BESTMILE', 'GSCOST', 'EPATMPG', 'EIADMPG', 'value']

textlist = ['HHSTATE', 'WRKTIME', 'HBHUR', 'HH_CBSA', 'HHC_MSA', 'TRIPPURP', 'MODLCODE', 'MAKECODE', 'MSN', 'Description', 'Unit']


def parser(tname, tpath):
    create_string = "CREATE TABLE " + tname + "("
    insert_string = "INSERT INTO " + tname + " VALUES ("
    f = open(tpath, "rt")
    reader = csv.reader(f)
    attri_list = reader.next()
    get_attr = reader.next()
    attri_listc = copy.deepcopy(attri_list)
    for i, element in enumerate(attri_list):
        if i == len(attri_list) - 1:
            attri_list[i] = attri_list[i] + " " + type_define(element)
            if type_define(element) == "text":
                insert_string += "'" + get_attr[i] + "'"
            else:
                insert_string += get_attr[i]
        else:
            attri_list[i] = attri_list[i] + " " + type_define(element) + ", "
            if type_define(element) == "text":
                insert_string += "'" + get_attr[i]+ "', "
            else:
                insert_string += get_attr[i] + ", "
    insert_string += ");"
    for attribute in attri_list:
        create_string += attribute
    create_string += ");"
    #conn = psycopg2.connect("dbname=hw4 user=postgres password=postgres host=localhost")
    conn = psycopg2.connect("dbname=postgres user =" + os.getlogin() + " host = localhost")
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS ' + tname + ";")
    cur.execute(create_string)
    print create_string
    print insert_string
    cur.execute(insert_string)
    for row in reader:
        new_ins = "INSERT INTO " + tname + " VALUES ("
        for i, element in enumerate(row):
            if i == len(get_attr) - 1:
                if type_define(attri_listc[i]) == "text":
                    new_ins += "'" + element + "'"
                elif element == 'Not Available':
                    new_ins += 'NULL'
                else:
                    new_ins += element
            else:
                if type_define(attri_listc[i]) == "text":
                    new_ins += "'" + element + "', "
                elif element == 'Not Available':
                    new_ins += 'NULL, '
                else:
                    new_ins += element + ", "
        new_ins += ");"
        print new_ins
        cur.execute(new_ins)
    conn.commit()
    cur.close()
    conn.close()


def main():
    parser("EIA_CO2_Transportation_2014", "EIA_CO2_Transportation_2014.csv")
    parser("EIA_MkWh_2014", "EIA_MkWh_2014.csv")
    parser("EIA_CO2_Electric_2014", "EIA_CO2_Electric_2014.csv")
    parser("DAYV2PUB", "Ascii\DAYV2PUB.CSV")
    parser("HHV2PUB", "Ascii\HHV2PUB.CSV")
    parser("VEHV2PUB", "Ascii\VEHV2PUB.CSV")

def type_define(whatis):
    ##print whatis
    for i in textlist:
        if whatis == i:
            return "text"

    for i in floatlist:
        if whatis == i:
            return "double precision"
    return "bigint"

if __name__ == '__main__':
    main()
