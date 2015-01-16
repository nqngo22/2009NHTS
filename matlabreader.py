__author__ = 'Shyam Pinnipati, Nguyen Ngo'

import h5py
import os
import psycopg2 as ps

floatlist = ['WTPERFIN', 'SFWGT', 'GCDWORK', 'DISTTOSC', 'TDCASEID', 'TRPMILES', 'WTTRDFIN', 'VMT_MILE', 'GASPRICE'
            , 'WTHHFIN', 'ANNMILES', 'VEHOWNMO', 'BESTMILE', 'GSCOST', 'EPATMPG', 'EIADMPG', 'value']

textlist = ['HHSTATE', 'WRKTIME', 'HBHUR', 'HH_CBSA', 'HHC_MSA', 'TRIPPURP', 'MODLCODE', 'MAKECODE', 'MSN', 'Description', 'Unit']

def type_define(whatis):
    ##print whatis
    for i in textlist:
        if whatis == i:
            return "text"

    for i in floatlist:
        if whatis == i:
            return "double precision"
    return "bigint"

def mat_uploader():
    mfile = h5py.File("HW4Data.mat", 'r')
    for f in mfile:
            create_string = "CREATE TABLE " + f + "("
            #print(f)
            data = mfile.get(f)
            save_string = " "
            attri_list = []
            attr_for_later = []
            for i, d in enumerate(data):
                attr_for_later.append(str(d))
                if i == len(data) - 1:
                    attri_list.append(str(d) + " " + type_define(d))
                else:
                    attri_list.append(str(d) + " " + type_define(d) + ', ')
            for attr in attri_list:
                create_string += attr
            create_string += ');'
            #print create_string
            #print len(attr_for_later)
            #conn = ps.connect("dbname=hw4 user=postgres password=postgres host=localhost")
            conn = ps.connect("dbname=postgres user =" + os.getlogin() + " host = localhost")
            cur = conn.cursor()
            cur.execute('DROP TABLE IF EXISTS ' + f + ";")
            #print(create_string)
            cur.execute(create_string)
            conn.commit()
            for i, d in enumerate(data):
                pass

            mylist = []
            a = {}
            k = 0
            while k < len(attr_for_later):
                a[k] = data.get(attr_for_later[k])
                mylist.append(a[k])
                k += 1

            ##s = (a[0], a[1], a[2], a[3])
            ##t = tuple(x[0] for x in s)
            ##mytuple = tuple(mylist)

            rowlimit = len(data.get(attr_for_later[0]))
            print rowlimit
            k = 0
            while k < rowlimit:
                t = tuple(x[k] for x in mylist)
                new_ins = "INSERT INTO " + f + " VALUES ("
                for i, element in enumerate(t):
                    if i == len(t) - 1:
                        if type_define(attr_for_later[i]) == "text":
                            new_ins += "'" + str(element) + "'"
                        elif element == 'Not Available':
                            new_ins += 'NULL'
                        else:
                            new_ins += str(element)
                    else:
                        if type_define(attr_for_later[i]) == "text":
                            new_ins += "'" + str(element) + "', "
                        elif element == 'Not Available':
                            new_ins += 'NULL, '
                        else:
                            new_ins += str(element) + ", "
                new_ins += ");"
                #print new_ins
                cur.execute(new_ins)
                k += 1
            #print("committing")
            conn.commit()

            cur.close()
            conn.close()


mat_uploader()



# for e in ele:
#     print(e)
# datar = np.array(data)
# for d in datar:
#     print(d)
