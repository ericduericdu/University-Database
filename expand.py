import csv 
import psycopg2
import os
import heapq
from datetime import datetime

#con_string = "dbname= 'postgres' user='postgres' password='251170'"
#con = psycopg2.connect(con_string)
con = psycopg2.connect(database="postgres", user=os.environ['USER'], port="5432")
cur = con.cursor()
try:
    given_cid = str(input("What is the CID? "))
    given_term = str(input("What is the term? "))
    student_num = int(input("How many students would you like to add? "))

    #(104, 'ABC')
    get_crse = """SELECT crse,subject
    FROM course
    WHERE cid = '%s' and term = '%s' or (cid = '%s' and term = '%s-1')"""

    cur.execute(get_crse % (given_cid,given_term,given_cid,given_term))
    con.commit()
    crse_subject = cur.fetchone()


    get_lecroom = """SELECT DISTINCT building,room
    FROM meeting
     WHERE cid = '%s' and term = '%s' and type = 'Lecture' 
     or (cid = '%s' and term = '%s-1' and type = 'Lecture')"""
    cur.execute(get_lecroom% (given_cid,given_term,given_cid,given_term))
    con.commit()
    lec_room = cur.fetchone()
    #('MB3', 1066) 

    get_secroom = """SELECT DISTINCT building,room
    FROM meeting
     WHERE cid = '%s' and term = '%s' and type <> 'Lecture' 
     or (cid = '%s' and term = '%s-1' and type <> 'Lecture')"""
    cur.execute(get_secroom% (given_cid,given_term,given_cid,given_term))
    con.commit()
    sec_room = cur.fetchone()
    #('MB3', 1066)

    lz= """ SELECT count(SID)
FROM (SELECT *
                FROM (course
                JOIN grade USING (cid, term)) JOIN meeting USING (CID, term)) all1
WHERE building = '%s' AND room = '%s' and term = '%s' or term ='%s-1'
GROUP BY room, building, crse, subject, term"""
    if lec_room:
        cur.execute(lz% (lec_room[0],lec_room[1],given_term,given_term))
        con.commit()
        lecture_size  = cur.fetchone()

    sz = """SELECT count(SID)
FROM (SELECT *
            FROM (course
                JOIN grade USING (cid, term)) JOIN meeting USING (CID, term)) all1
    WHERE building = '%s' and room = '%s' and cid = '%s' and term = '%s' or term = '%s-1'
      GROUP BY cid, term,building,room"""
    if sec_room:
        cur.execute(sz % (sec_room[0],sec_room[1],given_cid,given_term,given_term))
        con.commit()
        section_size = cur.fetchone()

    available = """SELECT *
    FROM ((SELECT building, room, max(maxcap) AS roomcap
           FROM (SELECT building, room, max(student_num) AS maxcap
                 FROM (SELECT subject, crse, term, building, room, count(sid) AS student_num
                       FROM (SELECT subject, crse, term, building, room, type
                             FROM course
                                 JOIN meeting USING (cid, term)
                            ) B
                           JOIN
                           (SELECT crse, term, sid
                            FROM grade
                                JOIN course USING (cid, term)
                           ) A
                           USING (crse, term)
                       WHERE type = 'Lecture'
                       GROUP BY B.subject, crse, term, building, room) C
                 GROUP BY building, room
                 UNION
                 SELECT building, room, max(student_number)
                 FROM (SELECT cid, term, count(sid) AS student_number
                       FROM grade
                           JOIN course USING (cid, term)
                       GROUP BY term, cid) A
                     JOIN meeting USING (cid, term)
                 GROUP BY building, room) D
           WHERE building IS NOT NULL AND room IS NOT NULL
           GROUP BY building, room) roomcap2
        JOIN

        (SELECT *
         FROM (SELECT room,building
               FROM meeting
               EXCEPT

               (SELECT B.room, B.building
                FROM (SELECT *
                      FROM meeting
                      WHERE cid = '%s' AND term = '%s') A, meeting B
                WHERE A.term = B.term AND A.start <= B.endtime AND B.start <= A.endtime AND A.day = B.day)
              ) avai) AS room_avai USING (room, building))"""

    cur.execute(available % (given_cid,given_term))
    con.commit()
    room_avai = cur.fetchall()
    # (1066, 'MB3', 18) 
    #ROOM BUILDING CAP
    clm = """SELECT max(count) AS roomcap
FROM (SELECT term, building, room, subject, crse, count(SID)
      FROM (SELECT *
            FROM (course
                JOIN grade USING (cid, term)) JOIN meeting USING (CID, term)) all1
      GROUP BY room, building, crse, subject, term
     ) rooms
WHERE building = '%s' AND room = '%s'
GROUP BY room, building"""

    #('MB3', 1066) 
    cur.execute(clm % (lec_room[0],lec_room[1]))
    con.commit()
    cur_lec_max = cur.fetchall()
    
    
    room_cap = {}
    lec_br  = str(lec_room[0])+ " " + str(lec_room[1])

    room_cap[lec_br] = cur_lec_max[0][0] 
    for r_b_c in room_avai:
        room_cap[str(r_b_c[1])+ " " + str(r_b_c[0])] = r_b_c[2]

    if sec_room:
        cur.execute(clm % (sec_room[0],sec_room[1]))
        con.commit()
        cur_sec_max = cur.fetchall()
        sec_br  = str(sec_room[0])+ " " + str(sec_room[1])
        room_cap[sec_br] = cur_sec_max[0][0]
        need_sec = section_size[0]+student_num
    
    h = []
    h_sec = []

    need_lec = lecture_size[0]+student_num
    if not sec_room and lec_room:
        print("Only Lecture needs to be moved")
        for key,value in room_cap.items():
            if value >= need_lec:
                heapq.heappush(h,key)
            if(len(h) == 5):
                break
    elif room_cap[lec_br] >= need_lec and room_cap[sec_br] >= need_sec:
        print("The current rooms can support the expansion")

    #if ONLY SECTION size is not big enough
    elif room_cap[sec_br] < need_sec and room_cap[lec_br] >= need_lec:
        print("Only Section needs to be moved")
        for key,value in room_cap.items():
            if value >= need_sec:
                heapq.heappush(h_sec,key)
                stopper += 1
            if(len(h_sec) == 5):
                break

    elif room_cap[lec_br] < need_lec and room_cap[sec_br] >= need_sec:
        print("Only Lecture needs to be moved")
        for key,value in room_cap.items():
            if value >= need_lec:
                heapq.heappush(h,key)
            if(len(h) == 5):
                break
    elif room_cap[sec_br] < need_sec and room_cap[lec_br] < need_lec:
        print("Both need to be moved")
        for key,value in room_cap.items():
            if value >= need_sec:
                heapq.heappush(h_sec,key)
            if value >= need_lec:
                heapq.heappush(h,key)
            if(len(h_sec) == 5 or len(h) == 5):
                break
    if len(h) > 0:
        print("Rooms suitable for Lecture")
        for i in range(len(h)):
            print("%d. %s" % (i+1, heapq.heappop(h) ))
        
    if len(h_sec) > 0:
        print("Rooms suitable for Section")
        for i in range(len(h_sec)):
            print("%d. %s" % (i+1, heapq.heappop(h_sec) ))

except KeyError:
    print("Course not found")

