__author__ = 'Shyam Pinnipati, Nguyen Ngo'

import psycopg2
import os

def query_one():
    answer_list = []
    for i in range(5, 105, 5):
        #conn = psycopg2.connect("dbname=hw4 user=postgres password=postgres host=localhost")
        conn = psycopg2.connect("dbname=postgres user =" + os.getlogin() + " host = localhost")
        cur = conn.cursor()
        cur.execute("SELECT(1.0 * (SELECT COUNT(*) \
FROM( \
SELECT HOUSEID, PERSONID, TDAYDATE, TRAVDAY, SUM(TRPMILES) \
FROM DAYV2PUB \
WHERE TRPMILES > -1 \
GROUP BY HOUSEID, PERSONID, TDAYDATE, TRAVDAY) t \
WHERE sum < " + str(i) + ")/ \
(SELECT COUNT(*) \
FROM(SELECT HOUSEID, PERSONID, TDAYDATE, TRAVDAY, SUM(TRPMILES) \
FROM DAYV2PUB \
WHERE TRPMILES > -1 \
GROUP BY HOUSEID, PERSONID, TDAYDATE, TRAVDAY) t))*100 as sol")
        conn.commit()
        j = cur.fetchone()[0]
        answer_list.append(j)
        cur.close()
        conn.close()
    print(answer_list)

def query_two():
    answer_list = []
    for i in range(5, 105, 5):
        #conn = psycopg2.connect("dbname=hw4 user=postgres password=postgres host=localhost")
        conn = psycopg2.connect("dbname=postgres user =" + os.getlogin() + " host = localhost")
        cur = conn.cursor()
        cur.execute("SELECT (SELECT SUM(VMT_MILE) FROM DAYV2PUB WHERE VEHID > 0 AND TRPMILES > 0 AND TRPMILES < " + str(i) + "  AND VMT_MILE > 0) / (SELECT SUM((VMT_MILE / EPATMPG)) \
                 FROM DAYV2PUB NATURAL JOIN (SELECT HOUSEID, VEHID, EPATMPG FROM VEHV2PUB) AS R1 \
                 WHERE VEHID > 0 AND TRPMILES > 0 AND TRPMILES < " + str(i) +" AND VMT_MILE > 0)")
        conn.commit()
        j = cur.fetchone()[0]
        answer_list.append(j)
        cur.close()
        conn.close()
    print(answer_list)

def query_three():
    #conn = psycopg2.connect("dbname=hw4 user=postgres password=postgres host=localhost")
    conn = psycopg2.connect("dbname=postgres user =" + os.getlogin() + " host = localhost")
    cur = conn.cursor()
    cur.execute("SELECT TDAYDATE, C02 * 100 * aveg / (1000000*EIA_CO2_Transportation_2014.value) as percent \
                 FROM(SELECT TDAYDATE, SUM(VMT_MILE / EPATMPG * 8.887 * 0.001 *30.4) as C02, 117538000/COUNT(DISTINCT HOUSEID) as aveg\
                 FROM DAYV2PUB NATURAL JOIN (SELECT HOUSEID, VEHID, EPATMPG FROM VEHV2PUB) AS R1 \
                 WHERE VEHID > 0 AND VMT_MILE > 0 AND TDAYDATE >=200803 AND TDAYDATE <= 200904 \
                 GROUP BY TDAYDATE \
                 ORDER BY TDAYDATE ASC) AS T, EIA_CO2_Transportation_2014 \
                 WHERE MSN = 'TEACEUS' AND TDAYDATE = YYYYMM;")
    conn.commit()
    print(cur.fetchall())
    cur.close()
    conn.close()

def query_four():
    #conn = psycopg2.connect("dbname=hw4 user=postgres password=postgres host=localhost")
    conn = psycopg2.connect("dbname=postgres user =" + os.getlogin() + " host = localhost")
    cur = conn.cursor()
    answer_list = []
    for i in range(20, 80, 20):
        cur.execute("SELECT t.TDAYDATE AS tdate, SUM(HybridCO2) AS Hybrid, SUM(CombustableCO2) AS Combust, SUM(HybridCO2)-SUM(CombustableCO2) AS CO2change \
                FROM (SELECT DAYV2PUB.TDAYDATE, \
                CASE \
                WHEN VMT_MILE >= " + str(i) + " THEN (((VMT_MILE - " + str(i) +")/EPATMPG) *8.887*0.001 + (EIA_CO2_ELECTRIC_2014.VALUE/EIA_MKWH_2014.VALUE)*(" + str(i) +"/(EPATMPG * 0.090634441))* 30.4) \
                ELSE ((EIA_CO2_ELECTRIC_2014.VALUE/EIA_MKWH_2014.VALUE)*(" + str(i) +"/(EPATMPG * 0.090634441))*30.4) \
                END \
                HybridCO2, (VMT_MILE/EPATMPG) * 8.887 * 0.001 AS CombustableCO2 \
                FROM DAYV2PUB, VEHV2PUB, EIA_MKWH_2014, EIA_CO2_ELECTRIC_2014 \
                WHERE DAYV2PUB.HOUSEID = VEHV2PUB.HOUSEID AND DAYV2PUB.VEHID = VEHV2PUB.VEHID AND DAYV2PUB.VEHID > 0 \
                AND EIA_CO2_ELECTRIC_2014.MSN = 'TXEIEUS' AND EIA_CO2_ELECTRIC_2014.YYYYMM = DAYV2PUB.TDAYDATE AND VMT_MILE>=0 \
                AND EIA_MKWH_2014.MSN='ELETPUS' AND EIA_MKWH_2014.YYYYMM = DAYV2PUB.TDAYDATE) t \
                GROUP BY tdate \
                ORDER BY tdate;")
        conn.commit()
        answer_list.append(cur.fetchall())
    print(answer_list)
    cur.close()
    conn.close()

def main():
    query_one() #3a
    query_two() #3b
    query_three() #3c
    query_four() #3d

if __name__ == '__main__':
    main()