import csv 
import psycopg2
import os
import time 
con = psycopg2.connect(database="postgres", user=os.environ['USER'], port="5432")
"""
con_string = "dbname= 'postgres' user='postgres' password='251170'"
con = psycopg2.connect(con_string)"""
cur = con.cursor()

sql5 = """SELECT *
FROM (
         (SELECT csub                                                                                    AS sub,
                 class203                                                                                AS crse,
                 psub                                                                                    AS Previous_Sub,
                 before203                                                                               AS Previous_CRSE,
                 CAST(ROUND(CAST((CAST(nStudents AS FLOAT4) / total203) * 100 AS NUMERIC), 2) AS FLOAT4) AS per_ofcourse
          FROM (SELECT csub, class203, psub, before203, count(before203) AS nStudents
                FROM (SELECT class203.subject AS csub, class203.crse AS class203, before203.subject AS psub,
                             before203.crse   AS before203
                      FROM (SELECT *
                            FROM grade
                                JOIN course USING (CID, term)
                            WHERE crse = 203 AND subject = 'ABC' AND length(term) < 7) class203
                          JOIN
                          (SELECT DISTINCT SID, subject, crse, term
                           FROM grade
                               JOIN course USING (CID, term)
                           WHERE ((crse <> 203 AND subject = 'ABC') OR (crse = 203 AND subject <> 'ABC') OR
                                  (crse <> 203 AND subject <> 'ABC')) AND length(term) < 7) before203
                              ON (class203.SID = before203.SID AND
                                  CAST(before203.term AS INT) < CAST(class203.term AS INT))) prereq
                GROUP BY before203, psub, csub, class203) totalother,
              (SELECT count(*) AS total203
               FROM (SELECT *
                     FROM grade
                         JOIN course USING (CID, term)
                     WHERE crse = 203 AND subject = 'ABC' AND length(term) < 7) taken203) numTaken203)
         UNION

         (SELECT csub                                                                                    AS sub,
                 class210                                                                                AS crse,
                 psub                                                                                    AS Previous_Sub,
                 before210                                                                               AS Previous_CRSE,
                 CAST(ROUND(CAST((CAST(nStudents AS FLOAT4) / total210) * 100 AS NUMERIC), 2) AS FLOAT4) AS per_ofcourse
          FROM (SELECT csub, class210, psub, before210, count(before210) AS nStudents
                FROM (SELECT class210.subject AS csub, class210.crse AS class210, before210.subject AS psub,
                             before210.crse   AS before210
                      FROM (SELECT *
                            FROM grade
                                JOIN course USING (CID, term)
                            WHERE crse = 210 AND subject = 'ABC' AND length(term) < 7) class210
                          JOIN
                          (SELECT DISTINCT SID, subject, crse, term
                           FROM grade
                               JOIN course USING (CID, term)
                           WHERE ((crse <> 210 AND subject = 'ABC') OR (crse = 210 AND subject <> 'ABC') OR
                                  (crse <> 210 AND subject <> 'ABC')) AND length(term) < 7) before210
                              ON (class210.SID = before210.SID AND
                                  CAST(before210.term AS INT) < CAST(class210.term AS INT))) prereq
                GROUP BY before210, psub, csub, class210) totalother,
              (SELECT count(*) AS total210
               FROM (SELECT *
                     FROM grade
                         JOIN course USING (CID, term)
                     WHERE crse = 210 AND subject = 'ABC' AND length(term) < 7) taken210) numTaken210)
         UNION

         (SELECT csub                                                                                    AS sub,
                 class222                                                                                AS crse,
                 psub                                                                                    AS Previous_Sub,
                 before222                                                                               AS Previous_CRSE,
                 CAST(ROUND(CAST((CAST(nStudents AS FLOAT4) / total222) * 100 AS NUMERIC), 2) AS FLOAT4) AS per_ofcourse
          FROM (SELECT csub, class222, psub, before222, count(before222) AS nStudents
                FROM (SELECT class222.subject AS csub, class222.crse AS class222, before222.subject AS psub,
                             before222.crse   AS before222
                      FROM (SELECT *
                            FROM grade
                                JOIN course USING (CID, term)
                            WHERE crse = 222 AND subject = 'ABC' AND length(term) < 7) class222
                          JOIN
                          (SELECT DISTINCT SID, subject, crse, term
                           FROM grade
                               JOIN course USING (CID, term)
                           WHERE ((crse <> 222 AND subject = 'ABC') OR (crse = 222 AND subject <> 'ABC') OR
                                  (crse <> 222 AND subject <> 'ABC')) AND length(term) < 7) before222
                              ON (class222.SID = before222.SID AND
                                  CAST(before222.term AS INT) < CAST(class222.term AS INT))) prereq
                GROUP BY before222, psub, csub, class222) totalother,
              (SELECT count(*) AS total222
               FROM (SELECT *
                     FROM grade
                         JOIN course USING (CID, term)
                     WHERE crse = 222 AND subject = 'ABC' AND length(term) < 7) taken222) numTaken222)
     ) xx
WHERE per_ofcourse >= 75
ORDER BY crse, per_ofCourse"""

cur.execute(sql5) 
con.commit()
answer = cur.fetchall()
p75to80 = []
p80to85= []
p85to90= []
p90to95= []
p95to100=[]

p75to80 = []
p80to85= []
p85to90= []
p90to95= []
p95to100=[]

p75to802 = []
p80to852= []
p85to902= []
p90to952= []
p95to1002 =[]

p75to803 = []
p80to853= []
p85to903= []
p90to953= []
p95to1003=[]

for row in answer:
	if row[1] == 203:
		if 75 <= row[4] and row[4] <80:
			p75to80.append(row)
		if 80 <= row[4] and row[4] <85:
			p80to85.append(row)
		if 85<=row[4] and row[4] <90:
			p85to90.append(row)
		if 90<=row[4] and row[4] <95:
			p90to95.append(row)
		if 95<=row[4] and row[4] <=100:
			p95to100.append(row)

	if row[1] == 210:
		if 75 <= row[4] and row[4] <80:
			p75to802.append(row)
		if 80 <= row[4] and row[4] <85:
			p80to852.append(row)
		if 85<=row[4] and row[4] <90:
			p85to902.append(row)
		if 90<=row[4] and row[4] <95:
			p90to952.append(row)
		if 95<=row[4] and row[4] <=100:
			p95to1002.append(row)

	if row[1] == 222:
		if 75 <= row[4] and row[4] <80:
			p75to803.append(row)
		if 80 <= row[4] and row[4] <85:
			p80to853.append(row)
		if 85<=row[4] and row[4] <90:
			p85to903.append(row)
		if 90<=row[4] and row[4] <95:
			p90to953.append(row)
		if 95<=row[4] and row[4] <=100:
			p95to1003.append(row)


print("*****ABC 203 Prerequisites*****")
print("75% to 80% have taken")
for i in p75to80:
  print(i[2],i[3],i[4])
print("\n")
print("80% to 85% have taken")
for i in p80to85:
  print(i[2],i[3],i[4])
print("\n")
print("85% to 90% have taken")
for i in p85to90:
  print(i[2],i[3],i[4])
print("\n")
print("90% to 95% have taken")
for i in p90to95:
  print(i[2],i[3],i[4])
print("\n")
print("95% to 100% have taken")
for i in p95to100:
  print(i[2],i[3],i[4])
print("\n")

print("*****ABC 210 Prequisites*****")
print("75% to 80% have taken")
for i in p75to802:
  print(i[2],i[3],i[4])
print("\n")
print("80% to 85% have taken")
for i in p80to852:
  print(i[2],i[3],i[4])
print("\n")
print("85% to 90% have taken")
for i in p85to902:
  print(i[2],i[3],i[4])
print("\n")
print("90% to 95% have taken")
for i in p90to952:
  print(i[2],i[3],i[4])
print("95% to 100% have taken")
for i in p95to1002:
  print(i[2],i[3],i[4])
print("\n")

print("*****ABC 222 Prerequisites*****")
print("75% to 80% have taken")
for i in p75to803:
  print(i[2],i[3],i[4])
print("\n")
print("80% to 85% have taken")
for i in p80to853:
  print(i[2],i[3],i[4])
print("\n")
print("85% to 90% have taken")
for i in p85to903:
  print(i[2],i[3],i[4])
print("\n")
print("90% to 95% have taken")
for i in p90to953:
  print(i[2],i[3],i[4])
print("\n")
print("95% to 100% have taken")
for i in p95to1003:
  print(i[2],i[3],i[4])
print("\n")
