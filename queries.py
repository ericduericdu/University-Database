import csv 
import psycopg2
import os
import time 


con = psycopg2.connect(database="postgres", user=os.environ['USER'], port="5432")
#con_string = "dbname= 'postgres' user='postgres'
#con = psycopg2.connect(con_string)
cur = con.cursor()

sql3a = """SELECT CAST(ROUND(CAST((CAST(indie.tu AS FLOAT8)/CAST(total.tu AS FLOAT8)) * 100 AS numeric), 2) AS float8) AS per
FROM (SELECT count(tUnits) AS tu
      FROM (SELECT
              term,
              sid,
              sum(unit) AS tUnits
            FROM grade
            GROUP BY SID, term) B
      WHERE tUnits = %s) indie,
  (SELECT count(tUnits) AS tu
   FROM (SELECT
           term,
           sid,
           sum(unit) AS tUnits
         FROM grade
         GROUP BY SID, term) A
   WHERE tUnits >= 1 AND tUnits <= 20) total"""


sql3b = """SELECT tUnits, CAST(ROUND(CAST(AVG(sumgpa/tunits) AS numeric), 2) AS FLOAT8) AS RealGPA
FROM (SELECT
        SID,
        term,
        SUM(GPA) AS sumGPA
      FROM (SELECT
              Term,
              SID,
              (CASE
               WHEN grade = 'A+'
                 THEN 4.0 * unit
               WHEN grade = 'A'
                 THEN 4.0 * unit
               WHEN grade = 'A-'
                 THEN 3.7 * unit
               WHEN grade = 'B+'
                 THEN 3.3 * unit
               WHEN grade = 'B'
                 THEN 3.0 * unit
               WHEN grade = 'B-'
                 THEN 2.7 * unit
               WHEN grade = 'C+'
                 THEN 2.3 * unit
               WHEN grade = 'C'
                 THEN 2.0 * unit
               WHEN grade = 'C-'
                 THEN 1.7 * unit
               WHEN grade = 'D+'
                 THEN 1.3 * unit
               WHEN grade = 'D'
                 THEN 1.0 * unit
               WHEN grade = 'D-'
                 THEN 0.7 * unit
               ELSE 0
               END) AS gpa
            FROM Grade
            WHERE (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                   grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                   grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                   grade = 'D+' OR grade = 'D' OR grade = 'D-' OR
                   grade = 'F')) nGPA
      GROUP BY SID, term) sum JOIN

      (SELECT
         term,
         sid,
         sum(unit) AS tUnits
       FROM grade
      WHERE (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
             grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
             grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
             grade = 'D+' OR grade = 'D' OR grade = 'D-' OR
             grade = 'F')
       GROUP BY SID, term) TotalUnits USING (term, sid)
WHERE tUnits >= 1 AND tUnits <= 20
GROUP BY tUnits
ORDER BY tUnits ASC
"""
sql3c = """SELECT *
FROM (SELECT instr, (sum/tUnits) as avgGPA
      FROM (SELECT *
            FROM (SELECT instr, SUM(nGPA) AS sum
                  FROM (SELECT instr,
                                (CASE
                                 WHEN grade = 'A+'
                                   THEN 4.0*unit
                                 WHEN grade = 'A'
                                   THEN 4.0*unit
                                 WHEN grade = 'A-'
                                   THEN 3.7*unit
                                 WHEN grade = 'B+'
                                   THEN 3.3*unit
                                 WHEN grade = 'B'
                                   THEN 3.0*unit
                                 WHEN grade = 'B-'
                                   THEN 2.7*unit
                                 WHEN grade = 'C+'
                                   THEN 2.3*unit
                                 WHEN grade = 'C'
                                   THEN 2.0*unit
                                 WHEN grade = 'C-'
                                   THEN 1.7*unit
                                 WHEN grade = 'D+'
                                   THEN 1.3*unit
                                 WHEN grade = 'D'
                                   THEN 1.0*unit
                                 WHEN grade = 'D-'
                                   THEN 0.7*unit
                                 ELSE 0
                                 END) AS nGPA
                              FROM grade JOIN course USING (cid, term)
                              WHERE instr <> '' AND
                                    (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                                     grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                                     grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                                     grade = 'D+' OR grade = 'D' OR grade = 'D-' OR
                                     grade = 'F')) sumGPA
                  GROUP BY instr) instrTotalGPA JOIN

                (SELECT instr, sum(unit) AS tUnits
                  FROM grade JOIN course USING (cid, term)
                  WHERE instr <> '' AND
                                    (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                                     grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                                     grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                                     grade = 'D+' OR grade = 'D' OR grade = 'D-' OR
                                     grade = 'F')
                  GROUP BY instr) instruTotalUnits USING (instr)) GPA) AllGPA
WHERE avgGPA = (SELECT max(avgGPA)
                FROM (SELECT instr, (sum/tUnits) as avgGPA
                      FROM (SELECT *
                            FROM (SELECT instr, SUM(nGPA) AS sum
                                  FROM (SELECT instr,
                                                (CASE
                                                 WHEN grade = 'A+'
                                                   THEN 4.0*unit
                                                 WHEN grade = 'A'
                                                   THEN 4.0*unit
                                                 WHEN grade = 'A-'
                                                   THEN 3.7*unit
                                                 WHEN grade = 'B+'
                                                   THEN 3.3*unit
                                                 WHEN grade = 'B'
                                                   THEN 3.0*unit
                                                 WHEN grade = 'B-'
                                                   THEN 2.7*unit
                                                 WHEN grade = 'C+'
                                                   THEN 2.3*unit
                                                 WHEN grade = 'C'
                                                   THEN 2.0*unit
                                                 WHEN grade = 'C-'
                                                   THEN 1.7*unit
                                                 WHEN grade = 'D+'
                                                   THEN 1.3*unit
                                                 WHEN grade = 'D'
                                                   THEN 1.0*unit
                                                 WHEN grade = 'D-'
                                                   THEN 0.7*unit
                                                 ELSE 0
                                                 END) AS nGPA
                                              FROM grade JOIN course USING (cid, term)
                                              WHERE instr <> '' AND
                                                    (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                                                     grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                                                     grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                                                     grade = 'D+' OR grade = 'D' OR grade = 'D-' OR
                                                     grade = 'F')) sumGPA
                                  GROUP BY instr) instrTotalGPA JOIN

                                (SELECT instr, sum(unit) AS tUnits
                                  FROM grade JOIN course USING (cid, term)
                                  WHERE instr <> '' AND
                                                    (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                                                     grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                                                     grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                                                     grade = 'D+' OR grade = 'D' OR grade = 'D-' OR
                                                     grade = 'F')
                                  GROUP BY instr) instruTotalUnits USING (instr)) GPA) max)
OR
  avggpa = (SELECT min(avgGPA)
            FROM (SELECT instr, (sum/tUnits) as avgGPA
                  FROM (SELECT *
                        FROM (SELECT instr, SUM(nGPA) AS sum
                              FROM (SELECT instr,
                                            (CASE
                                             WHEN grade = 'A+'
                                               THEN 4.0*unit
                                             WHEN grade = 'A'
                                               THEN 4.0*unit
                                             WHEN grade = 'A-'
                                               THEN 3.7*unit
                                             WHEN grade = 'B+'
                                               THEN 3.3*unit
                                             WHEN grade = 'B'
                                               THEN 3.0*unit
                                             WHEN grade = 'B-'
                                               THEN 2.7*unit
                                             WHEN grade = 'C+'
                                               THEN 2.3*unit
                                             WHEN grade = 'C'
                                               THEN 2.0*unit
                                             WHEN grade = 'C-'
                                               THEN 1.7*unit
                                             WHEN grade = 'D+'
                                               THEN 1.3*unit
                                             WHEN grade = 'D'
                                               THEN 1.0*unit
                                             WHEN grade = 'D-'
                                               THEN 0.7*unit
                                             ELSE 0
                                             END) AS nGPA
                                          FROM grade JOIN course USING (cid, term)
                                          WHERE instr <> '' AND
                                                (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                                                 grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                                                 grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                                                 grade = 'D+' OR grade = 'D' OR grade = 'D-' OR
                                                 grade = 'F')) sumGPA
                              GROUP BY instr) instrTotalGPA JOIN

                            (SELECT instr, sum(unit) AS tUnits
                              FROM grade JOIN course USING (cid, term)
                              WHERE instr <> '' AND
                                                (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                                                 grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                                                 grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                                                 grade = 'D+' OR grade = 'D' OR grade = 'D-' OR
                                                 grade = 'F')
                              GROUP BY instr) instruTotalUnits USING (instr)) GPA) min)
ORDER BY avggpa DESC"""

sql3d = """SELECT *
FROM
(SELECT ALLGPA.crse, ALLGPA.instr AS EasiestInstr, GPAofEasiest
FROM (SELECT crse, max(avggpa) AS GPAofEasiest
      FROM (SELECT instr, crse, sumofngpa/tunits AS avgGPA
            FROM (SELECT instr, crse, SUM(ngpa) AS SumofNGPA
                  FROM (SELECT crse, instr,
                        (CASE
                         WHEN grade = 'A+' THEN 4.0 * unit
                         WHEN grade = 'A'  THEN 4.0 * unit
                         WHEN grade = 'A-' THEN 3.7 * unit
                         WHEN grade = 'B+' THEN 3.3 * unit
                         WHEN grade = 'B'  THEN 3.0 * unit
                         WHEN grade = 'B-' THEN 2.7 * unit
                         WHEN grade = 'C+' THEN 2.3 * unit
                         WHEN grade = 'C'  THEN 2.0 * unit
                         WHEN grade = 'C-' THEN 1.7 * unit
                         WHEN grade = 'D+' THEN 1.3 * unit
                         WHEN grade = 'D'  THEN 1.0 * unit
                         WHEN grade = 'D-' THEN 0.7 * unit
                         ELSE 0
                         END) AS nGPA
                      FROM grade JOIN course USING (cid, term)
                      WHERE subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '' AND
                            (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                             grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                             grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                             grade = 'D+' OR grade = 'D' OR grade = 'D-' OR grade = 'F')) SumNGpa
                      GROUP BY instr, crse) totalSum JOIN

                  (SELECT instr, crse, SUM(unit) AS tUnits
                  FROM grade JOIN course USING (cid, term)
                  WHERE subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '' AND
                        (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                         grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                         grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                         grade = 'D+' OR grade = 'D' OR grade = 'D-' OR grade = 'F')
                  GROUP by instr, crse) totalUnits USING (instr, crse))avg
      GROUP BY crse) highlow JOIN

      (SELECT instr, crse, sumofngpa/tunits AS avgGPA
            FROM (SELECT instr, crse, SUM(ngpa) AS SumofNGPA
                  FROM (SELECT crse, instr,
                        (CASE
                         WHEN grade = 'A+' THEN 4.0 * unit
                         WHEN grade = 'A'  THEN 4.0 * unit
                         WHEN grade = 'A-' THEN 3.7 * unit
                         WHEN grade = 'B+' THEN 3.3 * unit
                         WHEN grade = 'B'  THEN 3.0 * unit
                         WHEN grade = 'B-' THEN 2.7 * unit
                         WHEN grade = 'C+' THEN 2.3 * unit
                         WHEN grade = 'C'  THEN 2.0 * unit
                         WHEN grade = 'C-' THEN 1.7 * unit
                         WHEN grade = 'D+' THEN 1.3 * unit
                         WHEN grade = 'D'  THEN 1.0 * unit
                         WHEN grade = 'D-' THEN 0.7 * unit
                         ELSE 0
                         END) AS nGPA
                      FROM grade JOIN course USING (cid, term)
                      WHERE subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '' AND
                            (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                             grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                             grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                             grade = 'D+' OR grade = 'D' OR grade = 'D-' OR grade = 'F')) SumNGpa
                      GROUP BY instr, crse) totalSum JOIN

                  (SELECT instr, crse, SUM(unit) AS tUnits
                  FROM grade JOIN course USING (cid, term)
                  WHERE subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '' AND
                        (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                         grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                         grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                         grade = 'D+' OR grade = 'D' OR grade = 'D-' OR grade = 'F')
                  GROUP by instr, crse) totalUnits USING (instr, crse))ALLGPA ON (highlow.GPAofEasiest = ALLGPA.avgGPA AND highlow.crse = ALLGPA.crse))high JOIN
(SELECT ALLGPA.crse, ALLGPA.instr AS HardestInstru, GPAofHardest
FROM (SELECT crse, min(avggpa) AS GPAofHardest
      FROM (SELECT instr, crse, sumofngpa/tunits AS avgGPA
            FROM (SELECT instr, crse, SUM(ngpa) AS SumofNGPA
                  FROM (SELECT crse, instr,
                        (CASE
                         WHEN grade = 'A+' THEN 4.0 * unit
                         WHEN grade = 'A'  THEN 4.0 * unit
                         WHEN grade = 'A-' THEN 3.7 * unit
                         WHEN grade = 'B+' THEN 3.3 * unit
                         WHEN grade = 'B'  THEN 3.0 * unit
                         WHEN grade = 'B-' THEN 2.7 * unit
                         WHEN grade = 'C+' THEN 2.3 * unit
                         WHEN grade = 'C'  THEN 2.0 * unit
                         WHEN grade = 'C-' THEN 1.7 * unit
                         WHEN grade = 'D+' THEN 1.3 * unit
                         WHEN grade = 'D'  THEN 1.0 * unit
                         WHEN grade = 'D-' THEN 0.7 * unit
                         ELSE 0
                         END) AS nGPA
                      FROM grade JOIN course USING (cid, term)
                      WHERE subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '' AND
                            (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                             grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                             grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                             grade = 'D+' OR grade = 'D' OR grade = 'D-' OR grade = 'F')) SumNGpa
                      GROUP BY instr, crse) totalSum JOIN

                  (SELECT instr, crse, SUM(unit) AS tUnits
                  FROM grade JOIN course USING (cid, term)
                  WHERE subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '' AND
                        (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                         grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                         grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                         grade = 'D+' OR grade = 'D' OR grade = 'D-' OR grade = 'F')
                  GROUP by instr, crse) totalUnits USING (instr, crse))avg
      GROUP BY crse) highlow JOIN

      (SELECT instr, crse, sumofngpa/tunits AS avgGPA
            FROM (SELECT instr, crse, SUM(ngpa) AS SumofNGPA
                  FROM (SELECT crse, instr,
                        (CASE
                         WHEN grade = 'A+' THEN 4.0 * unit
                         WHEN grade = 'A'  THEN 4.0 * unit
                         WHEN grade = 'A-' THEN 3.7 * unit
                         WHEN grade = 'B+' THEN 3.3 * unit
                         WHEN grade = 'B'  THEN 3.0 * unit
                         WHEN grade = 'B-' THEN 2.7 * unit
                         WHEN grade = 'C+' THEN 2.3 * unit
                         WHEN grade = 'C'  THEN 2.0 * unit
                         WHEN grade = 'C-' THEN 1.7 * unit
                         WHEN grade = 'D+' THEN 1.3 * unit
                         WHEN grade = 'D'  THEN 1.0 * unit
                         WHEN grade = 'D-' THEN 0.7 * unit
                         ELSE 0
                         END) AS nGPA
                      FROM grade JOIN course USING (cid, term)
                      WHERE subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '' AND
                            (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                             grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                             grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                             grade = 'D+' OR grade = 'D' OR grade = 'D-' OR grade = 'F')) SumNGpa
                      GROUP BY instr, crse) totalSum JOIN

                  (SELECT instr, crse, SUM(unit) AS tUnits
                  FROM grade JOIN course USING (cid, term)
                  WHERE subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '' AND
                        (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                         grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                         grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                         grade = 'D+' OR grade = 'D' OR grade = 'D-' OR grade = 'F')
                  GROUP by instr, crse) totalUnits USING (instr, crse))ALLGPA ON (highlow.GPAofHardest = ALLGPA.avgGPA AND highlow.crse = ALLGPA.crse)) low USING (crse)
ORDER BY crse ASC"""

sql3d2e = """ SELECT crse, instr, CAST(ROUND(CAST(maxPR.passrate AS NUMERIC), 2) AS FLOAT8) AS Passrate
FROM (SELECT crse, max(passrate) AS passrate
      FROM (SELECT crse, instr, CASt(nump AS FLOAT8) / CAST(numpnp AS FLOAT8) * 100 AS passrate
            FROM ((SELECT crse, instr, count(grade) AS numPNP
                   FROM (SELECT crse, instr, grade
                         FROM grade
                             JOIN course USING (cid, term)
                         WHERE (grade = 'P' OR grade = 'NP') AND subject = 'ABC' AND crse >= 100 AND crse <= 199 AND
                               instr <> '') a
                   GROUP BY instr, crse) PNP
                JOIN

                (SELECT crse, instr, count(grade) AS numP
                 FROM (SELECT crse, instr, grade
                       FROM grade
                           JOIN course USING (cid, term)
                       WHERE (grade = 'P') AND subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '') a
                 GROUP BY instr, crse) P USING (crse, instr)) PPNP NATURAL FULL OUTER JOIN

                (SELECT crse, instr, count(grade) AS numNP
                 FROM (SELECT crse, instr, grade
                       FROM grade
                           JOIN course USING (cid, term)
                       WHERE (grade = 'NP') AND subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '') a
                 GROUP BY instr, crse) NP) overall
      GROUP BY crse) maxPR
    JOIN

    (SELECT crse, instr, CASt(nump AS FLOAT8) / CAST(numpnp AS FLOAT8) * 100 AS passrate
     FROM ((SELECT crse, instr, count(grade) AS numPNP
            FROM (SELECT crse, instr, grade
                  FROM grade
                      JOIN course USING (cid, term)
                  WHERE (grade = 'P' OR grade = 'NP') AND subject = 'ABC' AND crse >= 100 AND crse <= 199 AND
                        instr <> '') a
            GROUP BY instr, crse) PNP
         JOIN

         (SELECT crse, instr, count(grade) AS numP
          FROM (SELECT crse, instr, grade
                FROM grade
                    JOIN course USING (cid, term)
                WHERE (grade = 'P') AND subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '') a
          GROUP BY instr, crse) P USING (crse, instr)) PPNP NATURAL FULL OUTER JOIN

         (SELECT crse, instr, count(grade) AS numNP
          FROM (SELECT crse, instr, grade
                FROM grade
                    JOIN course USING (cid, term)
                WHERE (grade = 'NP') AND subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '') a
          GROUP BY instr, crse) NP) allPR USING (crse, passrate)"""


sql3d2h = """    SELECT crse, instr AS hardest, CAST(ROUND(CAST(minPR.passrate AS NUMERIC), 2) AS FLOAT8) AS Passrate
FROM (SELECT crse, min(passrate) AS passrate
      FROM (SELECT crse, instr, CASt(nump AS FLOAT8) / CAST(numpnp AS FLOAT8) * 100 AS passrate
            FROM ((SELECT crse, instr, count(grade) AS numPNP
                   FROM (SELECT crse, instr, grade
                         FROM grade
                             JOIN course USING (cid, term)
                         WHERE (grade = 'P' OR grade = 'NP') AND subject = 'ABC' AND crse >= 100 AND crse <= 199 AND
                               instr <> '') a
                   GROUP BY instr, crse) PNP
                JOIN

                (SELECT crse, instr, count(grade) AS numP
                 FROM (SELECT crse, instr, grade
                       FROM grade
                           JOIN course USING (cid, term)
                       WHERE (grade = 'P') AND subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '') a
                 GROUP BY instr, crse) P USING (crse, instr)) PPNP NATURAL FULL OUTER JOIN

                (SELECT crse, instr, count(grade) AS numNP
                 FROM (SELECT crse, instr, grade
                       FROM grade
                           JOIN course USING (cid, term)
                       WHERE (grade = 'NP') AND subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '') a
                 GROUP BY instr, crse) NP) overall
      GROUP BY crse) minPR
    JOIN

    (SELECT crse, instr, CASt(nump AS FLOAT8) / CAST(numpnp AS FLOAT8) * 100 AS passrate
     FROM ((SELECT crse, instr, count(grade) AS numPNP
            FROM (SELECT crse, instr, grade
                  FROM grade
                      JOIN course USING (cid, term)
                  WHERE (grade = 'P' OR grade = 'NP') AND subject = 'ABC' AND crse >= 100 AND crse <= 199 AND
                        instr <> '') a
            GROUP BY instr, crse) PNP
         JOIN

         (SELECT crse, instr, count(grade) AS numP
          FROM (SELECT crse, instr, grade
                FROM grade
                    JOIN course USING (cid, term)
                WHERE (grade = 'P') AND subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '') a
          GROUP BY instr, crse) P USING (crse, instr)) PPNP NATURAL FULL OUTER JOIN

         (SELECT crse, instr, count(grade) AS numNP
          FROM (SELECT crse, instr, grade
                FROM grade
                    JOIN course USING (cid, term)
                WHERE (grade = 'NP') AND subject = 'ABC' AND crse >= 100 AND crse <= 199 AND instr <> '') a
          GROUP BY instr, crse) NP) allPR USING (crse, passrate)"""

sql3e = """SELECT DISTINCT subject, crse, cid
FROM course
WHERE length(term) > 7
ORDER BY subject,crse"""

sql3f = """SELECT *
FROM (SELECT major, (CAST(sum AS float8)/CAST(majortotalunits AS float8)) AS GPA
      FROM (SELECT major, SUM(weight)
            FROM (SELECT major,
                          (CASE
                                 WHEN grade = 'A+'
                                   THEN 4.0 * unit
                                 WHEN grade = 'A'
                                   THEN 4.0 * unit
                                 WHEN grade = 'A-'
                                   THEN 3.7 * unit
                                 WHEN grade = 'B+'
                                   THEN 3.3 * unit
                                 WHEN grade = 'B'
                                   THEN 3.0 * unit
                                 WHEN grade = 'B-'
                                   THEN 2.7 * unit
                                 WHEN grade = 'C+'
                                   THEN 2.3 * unit
                                 WHEN grade = 'C'
                                   THEN 2.0 * unit
                                 WHEN grade = 'C-'
                                   THEN 1.7 * unit
                                 WHEN grade = 'D+'
                                   THEN 1.3 * unit
                                 WHEN grade = 'D'
                                   THEN 1.0 * unit
                                 WHEN grade = 'D-'
                                   THEN 0.7 * unit
                                 ELSE 0
                                 END) AS weight
                  FROM (course JOIN grade USING (cid, term)) JOIN student_major USING (sid, term)
                  WHERE subject = 'ABC' AND
                       (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                        grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                        grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                        grade = 'D+' OR grade = 'D' OR grade = 'D-' OR
                        grade = 'F')) majorTotalWeight
            GROUP BY major) MajorTotal1 JOIN
            (SELECT major, SUM(unit) AS majorTotalUnits
            FROM (SELECT major, unit
                  FROM (course JOIN grade USING (cid, term)) JOIN student_major USING (sid, term)
                  WHERE subject = 'ABC' AND
                             (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                              grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                              grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                              grade = 'D+' OR grade = 'D' OR grade = 'D-' OR
                              grade = 'F')) majorTotalUnits
            GROUP BY major) MajorTotal2 USING (major)) MajorGPA
WHERE gpa = (SELECT max(gpa)
            FROM (SELECT major, (CAST(sum AS float8)/CAST(majortotalunits AS float8)) AS GPA
                        FROM (SELECT major, SUM(weight)
                              FROM (SELECT major,
                                            (CASE
                                                   WHEN grade = 'A+'
                                                     THEN 4.0 * unit
                                                   WHEN grade = 'A'
                                                     THEN 4.0 * unit
                                                   WHEN grade = 'A-'
                                                     THEN 3.7 * unit
                                                   WHEN grade = 'B+'
                                                     THEN 3.3 * unit
                                                   WHEN grade = 'B'
                                                     THEN 3.0 * unit
                                                   WHEN grade = 'B-'
                                                     THEN 2.7 * unit
                                                   WHEN grade = 'C+'
                                                     THEN 2.3 * unit
                                                   WHEN grade = 'C'
                                                     THEN 2.0 * unit
                                                   WHEN grade = 'C-'
                                                     THEN 1.7 * unit
                                                   WHEN grade = 'D+'
                                                     THEN 1.3 * unit
                                                   WHEN grade = 'D'
                                                     THEN 1.0 * unit
                                                   WHEN grade = 'D-'
                                                     THEN 0.7 * unit
                                                   ELSE 0
                                                   END) AS weight
                                    FROM (course JOIN grade USING (cid, term)) JOIN student_major USING (sid, term)
                                    WHERE subject = 'ABC' AND
                                         (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                                          grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                                          grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                                          grade = 'D+' OR grade = 'D' OR grade = 'D-' OR
                                          grade = 'F')) majorTotalWeight
                              GROUP BY major) MajorTotal1 JOIN
                              (SELECT major, SUM(unit) AS majorTotalUnits
                              FROM (SELECT major, unit
                                    FROM (course JOIN grade USING (cid, term)) JOIN student_major USING (sid, term)
                                    WHERE subject = 'ABC' AND
                                               (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                                                grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                                                grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                                                grade = 'D+' OR grade = 'D' OR grade = 'D-' OR
                                                grade = 'F')) majorTotalUnits
                              GROUP BY major) MajorTotal2 USING (major)) max)
  OR
  gpa = (SELECT min(gpa)
        FROM (SELECT major, (CAST(sum AS float8)/CAST(majortotalunits AS float8)) AS GPA
                    FROM (SELECT major, SUM(weight)
                          FROM (SELECT major,
                                        (CASE
                                               WHEN grade = 'A+'
                                                 THEN 4.0 * unit
                                               WHEN grade = 'A'
                                                 THEN 4.0 * unit
                                               WHEN grade = 'A-'
                                                 THEN 3.7 * unit
                                               WHEN grade = 'B+'
                                                 THEN 3.3 * unit
                                               WHEN grade = 'B'
                                                 THEN 3.0 * unit
                                               WHEN grade = 'B-'
                                                 THEN 2.7 * unit
                                               WHEN grade = 'C+'
                                                 THEN 2.3 * unit
                                               WHEN grade = 'C'
                                                 THEN 2.0 * unit
                                               WHEN grade = 'C-'
                                                 THEN 1.7 * unit
                                               WHEN grade = 'D+'
                                                 THEN 1.3 * unit
                                               WHEN grade = 'D'
                                                 THEN 1.0 * unit
                                               WHEN grade = 'D-'
                                                 THEN 0.7 * unit
                                               ELSE 0
                                               END) AS weight
                                FROM (course JOIN grade USING (cid, term)) JOIN student_major USING (sid, term)
                                WHERE subject = 'ABC' AND
                                     (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                                      grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                                      grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                                      grade = 'D+' OR grade = 'D' OR grade = 'D-' OR
                                      grade = 'F')) majorTotalWeight
                          GROUP BY major) MajorTotal1 JOIN
                          (SELECT major, SUM(unit) AS majorTotalUnits
                          FROM (SELECT major, unit
                                FROM (course JOIN grade USING (cid, term)) JOIN student_major USING (sid, term)
                                WHERE subject = 'ABC' AND
                                           (grade = 'A+' OR grade = 'A' OR grade = 'A-' OR
                                            grade = 'B+' OR grade = 'B' OR grade = 'B-' OR
                                            grade = 'C+' OR grade = 'C' OR grade = 'C-' OR
                                            grade = 'D+' OR grade = 'D' OR grade = 'D-' OR
                                            grade = 'F')) majorTotalUnits
                          GROUP BY major) MajorTotal2 USING (major)) min)
ORDER BY gpa DESC"""

sql3g = """SELECT CAST(count(numtransabc) AS FLOAT8) / CAST(count(numabc) AS FLOAT8) * 100 AS per_transferred
FROM (SELECT count(*)
      FROM ((SELECT DISTINCT abc.sid
             FROM (SELECT *
                   FROM student_major
                   WHERE major LIKE 'ABC%') abc,
                 (SELECT *
                  FROM student_major
                  WHERE major NOT LIKE 'ABC%') notabc
             WHERE abc.sid = notabc.sid AND
                   cast(substring(abc.term FROM 1 FOR 6) AS INT) > cast(substring(notabc.term FROM 1 FOR 6) AS INT))

            EXCEPT

            (SELECT DISTINCT abc.sid
             FROM (SELECT *
                   FROM student_major
                   WHERE major LIKE 'ABC%') abc,
                 (SELECT *
                  FROM student_major
                  WHERE major NOT LIKE 'ABC%') notabc
             WHERE abc.sid = notabc.sid AND
                   cast(substring(abc.term FROM 1 FOR 6) AS INT) > cast(substring(notabc.term FROM 1 FOR 6) AS INT) AND
                   cast(substring(abc.term FROM 1 FOR 6) AS INT) <
                   cast(substring(notabc.term FROM 1 FOR 6) AS INT))) TranABCstayed) numtransabc,

    (SELECT count(*)
     FROM ((SELECT DISTINCT sid
            FROM student_major
            WHERE major LIKE 'ABC%')

           EXCEPT

           (SELECT DISTINCT abc.sid
            FROM (SELECT *
                  FROM student_major
                  WHERE major LIKE 'ABC%') abc,
                (SELECT *
                 FROM student_major
                 WHERE major NOT LIKE 'ABC%') notabc
            WHERE abc.sid = notabc.sid AND
                  cast(substring(abc.term FROM 1 FOR 6) AS INT) < cast(substring(notabc.term FROM 1 FOR 6) AS INT) AND
                  NOT (cast(substring(abc.term FROM 1 FOR 6) AS INT) >
                       cast(substring(notabc.term FROM 1 FOR 6) AS INT)))) ABCbegend) numabc"""

sql3g2 = """ SELECT major, ROUND(CAST((numStudents/total)*100 AS numeric), 2) AS percent
FROM(SELECT major, count(major) AS numStudents
      FROM (SELECT DISTINCT other.SID, other.major
            FROM (SELECT term, SID, major
                  FROM student_major
                  WHERE major NOT LIKE 'ABC%')other,
                 (SELECT term, SID, major
                  FROM student_major
                  WHERE major LIKE 'ABC%')abc
            WHERE abc.SID = other.SID AND length(abc.term) < 7 AND length(other.term) < 7 AND cast(other.term AS INT) < cast(abc.term AS INT)) intoABC
      GROUP BY major
      ORDER BY numStudents DESC
      LIMIT 5) TotalofEachTop5,
    (SELECT sum(numStudents) AS total
      FROM(SELECT major, count(major) AS numStudents
            FROM (SELECT DISTINCT other.SID, other.major
                  FROM (SELECT term, SID, major
                        FROM student_major
                        WHERE major NOT LIKE 'ABC%')other,
                       (SELECT term, SID, major
                        FROM student_major
                        WHERE major LIKE 'ABC%')abc
                  WHERE abc.SID = other.SID AND length(abc.term) < 7 AND length(other.term) < 7 AND cast(other.term AS INT) < cast(abc.term AS INT)) intoABC
            GROUP BY major) TotalofEach5)TotalofAll5"""
print("**********************3a*********************************************")
ans3a = {}
print("Unit","Percentage")
for i in range(1,21):
	cur.execute(sql3a % i)
	con.commit()
	row = cur.fetchone()
	ans3a[i] = row
	print(i,row[0]) 
print("************************3b*******************************************")		
cur.execute(sql3b) 
colnames = [desc[0] for desc in cur.description]
print(colnames[0],colnames[1])
con.commit()
a3b = cur.fetchall()
for i in range(len(a3b)):
	print(a3b[i][0],a3b[i][1])
print("************************3c*******************************************")
cur.execute(sql3c) 
colnames = [desc[0] for desc in cur.description]
print(colnames[0],colnames[1])
con.commit()
a3c = cur.fetchall()
for i in a3c:
	print(i[0],i[1])
print("*************************3d letter grade******************************************")
#ROUND ANN PUT TITLE AND REFORMAT D2
cur.execute(sql3d) 
colnames = [desc[0] for desc in cur.description]
print(colnames[0],colnames[1],colnames[2],colnames[3],colnames[4])
con.commit()
a3d = cur.fetchall()
for i in a3d:
	print(i[0],i[1],round(i[2],2),i[3],round(i[4],2))
print("****************************3d easiest passrate***************************************")
#EASIEST FOR 3 D part 2
cur.execute(sql3d2e)
colnames = [desc[0] for desc in cur.description]
a3d2e = cur.fetchall()
print(colnames[0],colnames[1],colnames[2])
a3d2e = sorted(a3d2e, key=lambda a3d2e: a3d2e[0])
for i in a3d2e:
  print(i[0],i[1],i[2])
print("*******************************3d hardest pass rate************************************")
#HARDEST FOR 3 D part 2
cur.execute(sql3d2h)
colnames = [desc[0] for desc in cur.description]
a3d2h = cur.fetchall()
print(colnames[0],colnames[1],colnames[2])
a3d2h = sorted(a3d2h, key=lambda a3d2h: a3d2h[0])
for i in a3d2h:
  print(i[0],i[1],i[2])
print("*****************************3e**************************************")
#QUESTION 3 E
cur.execute(sql3e) 
colnames = [desc[0] for desc in cur.description]
print(colnames[0], colnames[1],colnames[2])
con.commit()
a3e = cur.fetchall()
for i in a3e:
	print(i[0], i[1],i[2])
print("******************************3f*************************************")
#QUESTION 3 F
cur.execute(sql3f) 
colnames = [desc[0] for desc in cur.description]
print(colnames[0],colnames[1])
con.commit()
a3f = cur.fetchall()
for i in a3f:
	print(i[0],i[1])
#Question 3G 
print("***************************3g % transferred ABC****************************************")
cur.execute(sql3g) 
colnames = [desc[0] for desc in cur.description]
print(colnames[0])
con.commit()
a3g = cur.fetchall()
print(round(a3g[0][0],2))
print("*********************************3g Majors to ABC**********************************")
#Question 3G part 2
cur.execute(sql3g2) 
colnames = [desc[0] for desc in cur.description]
print(colnames[0],colnames[1])
con.commit()
a3g2 = cur.fetchall()
for i in a3g2:
	print(i[0],round(i[1],2))



