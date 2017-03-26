import csv 
import psycopg2
import os
import glob
from datetime import datetime

#con = psycopg2.connect(database="postgres", user=os.environ['USER'], port="5432")
con_string = "dbname= 'postgres' user='postgres' password='251170'"
con = psycopg2.connect(con_string)
cur = con.cursor()

def create_tables():
    commands = (
        """
        CREATE TABLE student_info (
            sid INT PRIMARY KEY,
            email VARCHAR(355) UNIQUE NOT NULL,
            prefname VARCHAR(50) NOT NULL,
            surname VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE student_major (
            sid INT ,
            term VARCHAR(20) NOT NULL,
            major Varchar(7) NOT NULL,
            PRIMARY KEY(sid,term)
        )
        """,
        """
        CREATE TABLE student_class (
            sid INT references student_info(sid),
            term VARCHAR(20)NOT NULL, 
            class VARCHAR(20) NOT NULL,
            PRIMARY KEY(sid,term)
        )
        """,
        """
        CREATE TABLE student_type (
            class VARCHAR(20) PRIMARY KEY,
            level VARCHAR(20) NOT NULL
        )
        """,
        """
        CREATE TABLE grade(
            sid INT references student_info(sid),
            cid INT NOT NULL,
            term VARCHAR(20) NOT NULL,
            status VARCHAR(5),
            grade VARCHAR(5),
            unit REAL,
            PRIMARY KEY (sid,cid,term)
        )
        """,
        """
        CREATE TABLE  course(
            cid INT NOT NULL,
            term VARCHAR(20) NOT NULL,
            subject VARCHAR(20),
            min_unit REAL,
            max_unit REAL,
            instr VARCHAR(200),
            crse INT,
            PRIMARY KEY(cid,term)
        )
        """,
        """
        CREATE TABLE  section(
            cid INT,
            section INT,
            term VARCHAR(20),
            PRIMARY KEY (cid,term)
        )
        """,
        """
        CREATE TABLE  meeting(
            cid INT,
            term VARCHAR(20),
            day VARCHAR(5),
            room INT,
            building VARCHAR(50),
            start time,
            endtime time, 
            type VARCHAR(50),
            PRIMARY KEY (cid,term,day,type),
            UNIQUE (CID,term,day,start,endtime)
        )
        """
        )
    for command in commands:
        cur.execute(command)

    con.commit()
    
    return commands;

"""http://stackoverflow.com/questions/4595197"""
def group_by_heading( some_source ):
    buffer= []
    for line in some_source:
        if(line == ['']):
            if buffer: yield buffer
            buffer= [ line ]
        else:
            buffer.append(line )
    yield buffer

def process_file(files_to_read):
    heading = []
    data = []

    for file_name in files_to_read:
        with open(file_name, "r") as csvfile:
            try:
                readCSV = csv.reader(csvfile,delimiter=',') 
                for file in group_by_heading(readCSV):
                    heading.append(file[1])
                    data.append(file[2:])
            except IndexError:
                pass
    return heading,data

def time_converter(time):
    u = time.split("-")
    start = u[0].rstrip() 
    end = u[1].strip()
    tformat = '%I:%M %p'
    start = datetime.strptime(start, tformat)
    end = datetime.strptime(end, tformat)
    start = start.time().isoformat()
    end = end.time().isoformat()
    return start,end

#LEVEL, CLASS, MAJOR
def check_student(students,courses):
    student_hash = {}
    student_conflict = []
    sid_conflict = []
    for i in range(len(students)):
        for j in range(len(students[i])):
            if(courses[i][0][1][-1] != "6"):
                continue
            sid_term = str(students[i][j][1])+str(courses[i][0][1])
            if sid_term in student_hash:
                
                if students[i][j][4] != student_hash[sid_term][0]: #Different Level
                    sid_conflict.append(students[i][j][1])
                    sid_conflict.append(student_hash[sid_term][5])
                    student_conflict.append(i)
                    student_conflict.append(student_hash[sid_term][4])
                if students[i][j][6] != student_hash[sid_term][1]: #Different Class
                    sid_conflict.append(students[i][j][1])
                    sid_conflict.append(student_hash[sid_term][5])
                    student_conflict.append(i)
                    student_conflict.append(student_hash[sid_term][4])
                if students[i][j][7] != student_hash[sid_term][2]: #Different Major
                    sid_conflict.append(students[i][j][1])
                    sid_conflict.append(student_hash[sid_term][5])
                    student_conflict.append(i)
                    student_conflict.append(student_hash[sid_term][4])
                if students[i][j][9] != student_hash[sid_term][3]: #Different Status
                    sid_conflict.append(students[i][j][1])
                    sid_conflict.append(student_hash[sid_term][5])
                    student_conflict.append(i)
                    student_conflict.append(student_hash[sid_term][4])

            else:
                student_hash[sid_term] = (students[i][j][4],students[i][j][6],students[i][j][7],students[i][j][9],i,students[i][j][1])
        
    return set(student_conflict),set(sid_conflict)

def summer_conflict(Course,locations):
    summer = {}
    summer2 = {}
    conflict = []
    for i in range(len(locations)):
        for j in range(len(locations[i])):
            if(Course[i][0][1][-1] != "6"):
                continue
            if (locations[i][j][2] == ""):
                continue
            start,end = time_converter(locations[i][j][3])
            for day in locations[i][j][2]:
                summer_hash = str(Course[i][0][1]) + str(locations[i][j][4]) \
                 + str(locations[i][j][5]) + day
                if summer_hash in summer:
                    start2 = summer[summer_hash][-2]
                    end2 = summer[summer_hash][-1]
                    if(Course[i][0][2:4] != summer[summer_hash][0]):
                        if(start < end2 and start2 < end):
                            conflict.append(summer[summer_hash][1])
                            conflict.append(i)   
                        if(start == start2 or end == end2):    
                            conflict.append(summer[summer_hash][1])
                            conflict.append(i) 
                    else:
                        summer2[summer_hash] = (Course[i][0][2:4],i,start,end)
                    if summer_hash in summer2:
                        start3 = summer2[summer_hash][-2]
                        end3 = summer2[summer_hash][-1]
                        if(Course[i][0][2:4] != summer2[summer_hash][0]):
                            if(start < end3 and start3 < end):
                                conflict.append(summer2[summer_hash][1])
                                conflict.append(i)   
                            if(start == start3 or end == end3): 
                                conflict.append(summer2[summer_hash][1])   
                                conflict.append(i)
                else:
                    summer[summer_hash] = (Course[i][0][2:4],i,start,end)

    return set(conflict)

def divide_file(data):
    CID = []
    INSTRUCTOR = []
    Student = []
    for i in range(0,len(data),3):
        if not data[2+i]: #Remove courses with no students
            continue
        else:
            CID.append(data[0+i])
            INSTRUCTOR.append(data[1+i])
            Student.append(data[2+i])
    
       
    return CID,INSTRUCTOR,Student

def generate_file():
    files_to_read = []
    files_to_read.append("1989_Q3.csv")
    files_to_read.append("1989_Q4.csv")
    for year in range(1990,2012):
        for quarter in range(1,5):
            file_name = str(year) + '_' + 'Q' + str(quarter) + ".csv"
            files_to_read.append(file_name)
    for quarter in range(1,4):
            file_name = str(2012) + '_' + 'Q' + str(quarter) + ".csv"
            files_to_read.append(file_name)
    return files_to_read


def unit_filter(var_unit):
    if not var_unit:
        return None,None
    u = var_unit.split("-")
    if len(u) == 1:
        min_unit = u[0]
        max_unit = min_unit
    else:
        min_unit = u[0] 
        max_unit  = u[1]
    return min_unit,max_unit

def time_filter(time):
    if not time:
        return None,None
    u = time.split("-")
    start = u[0] 
    end = u[1]
    return start,end

def clean(students,courses,locations):
    for i in range(len(students)):
        if not locations[i][0][0]:
            locations[i][0][0] = None
        for j in range(len(students[i])):
            for k in range(len(students[i][j])):
                if not students[i][j][k]:
                    students[i][j][k] = None
            
    return 

def insert_students(students,courses,sid_conflict):
    student_info = []
    student_major = []
    student_type = []
    student_class = []
    standing = []
    info = {}
    student_hash = {}
    for i in range(len(students)):
        for j in range(len(students[i])):
            hashkey = str(students[i][j][1])+str(courses[i][0][1])
            student_hashkey = str(students[i][j][1])
            if student_hashkey not in student_hash:
                student_hash[student_hashkey] = 1
                student_info.append( (students[i][j][1],students[i][j][2],students[i][j][3],students[i][j][10]))
            if hashkey in info:
                continue
            else:
                info[str(students[i][j][1])+str(courses[i][0][1])] = students[i][j]

                if students[i][j][1] in sid_conflict:
                    term = str(courses[i][0][1]) + "-1"
                else:
                    term = str(courses[i][0][1])

                student_major.append( (students[i][j][1],term,students[i][j][7]))
                student_class.append( (students[i][j][1],term,students[i][j][6]) ) 
                

    for i in range(len(students)):
        for j in range(len(students[i])):
            if students[i][j][6] in info:
                continue
            else:
                info[students[i][j][6]] = 1
                student_type.append( (students[i][j][6],students[i][j][4])) 

    """http://stackoverflow.com/questions/8134602"""
    records_list_template = ','.join(['%s'] * len(student_info))
    insert_query = 'INSERT INTO student_info (sid, surname,prefname,email) VALUES{0}'.format(records_list_template)
    cur.execute(insert_query,student_info)

    records_list_template = ','.join(['%s'] * len(student_major))
    insert_query = 'INSERT INTO student_major (sid,term,major) VALUES{0}'.format(records_list_template)
    cur.execute(insert_query,student_major)

    records_list_template = ','.join(['%s'] * len(student_class))
    insert_query = 'INSERT INTO student_class (sid,term, class) VALUES{0}'.format(records_list_template)
    cur.execute(insert_query,student_class)

    records_list_template = ','.join(['%s'] * len(student_type))
    insert_query = 'INSERT INTO student_type (class,level) VALUES{0}'.format(records_list_template)
    cur.execute(insert_query,student_type)  

    con.commit()

    return info
def insert_course(courses,students,locations,sc):
    grade = []
    course = []
    section = []
    course_info = {}
    standing_info = {}
    grade_info = {}

    for i in sc:
        courses[i][0][1] += "-1"
    for i in range(len(students)):
        for j in range(len(students[i])):
            hashkey = str(students[i][j][1])+str(courses[i][0][1])
            minU,maxU = unit_filter(courses[i][0][5])
            course_hashkey = str(courses[i][0][0]) + " " + str(courses[i][0][1])
            if course_hashkey in course_info:
                continue
            else: 
                course_info[course_hashkey] = locations[i]
                section.append( (courses[i][0][0],courses[i][0][4],courses[i][0][1]))
                course.append( (courses[i][0][0],courses[i][0][1],courses[i][0][2],minU,maxU,locations[i][0][0],courses[i][0][3]))

    
    for i in range(len(students)):
        for j in range(len(students[i])):
            grade_hashkey = str(students[i][j][1])+ " " + str(courses[i][0][0]) + " " + str(courses[i][0][1])
            if grade_hashkey in grade_info:
                continue
            else:
                grade_info[grade_hashkey] = 1
                
                grade.append((students[i][j][1],courses[i][0][0],courses[i][0][1],students[i][j][9],students[i][j][8],students[i][j][5]))

    records_list_template = ','.join(['%s'] * len(grade))
    insert_query = 'INSERT INTO grade (sid, cid,term,status,grade,unit) VALUES{0}'.format(records_list_template)
    cur.execute(insert_query,grade)

    records_list_template = ','.join(['%s'] * len(course))
    insert_query = 'INSERT INTO course (cid,term,subject,min_unit,max_unit,instr,crse) VALUES{0}'.format(records_list_template)
    cur.execute(insert_query,course)

    records_list_template = ','.join(['%s'] * len(section))
    insert_query = 'INSERT INTO section(cid,section,term) VALUES{0}'.format(records_list_template)
    cur.execute(insert_query,section)

    con.commit()
    return course_info

def insert_location(courses,locations):
    room_cap = []
    room_info = {}
    meeting = []
    meeting_info = {}

    for i in range(len(locations)):
        for k in range(len(locations[i])):
            room_hashkey = locations[i][k][4] + " " + str(locations[i][k][5])

            start_time, end_time = time_filter(locations[i][k][3])
            meeting_hash = str(courses[i][0][0])+str(courses[i][0][1])+str(locations[i][k][2])
            
            if (locations[i][k][2] != ""):
                if not locations[i][k][4]:
                    locations[i][k][4] = None
                    locations[i][k][5] = None
                if meeting_hash not in meeting_info:
                    meeting_info[meeting_hash] = 1
                    meeting.append( (courses[i][0][0],courses[i][0][1],locations[i][k][2],locations[i][k][5],locations[i][k][4],start_time,end_time,locations[i][k][1]))

    records_list_template = ','.join(['%s'] * len(meeting))
    insert_query = 'INSERT INTO meeting  (cid,term,day,room,building,start,endtime,type) VALUES{0}'.format(records_list_template)
    cur = con.cursor()
    cur.execute(insert_query,meeting)

    cur.close()
    con.commit()

    return 


#Student[]<-Section Number
#Student[][]<-- Seat # within that section
#Student[][][]<--The attribute with that row 

#Course[]<-Section Number
#Course[][]<-- Course within the file
#Course[][][]<--The attribute with that row 
#<----------start of main ----------------->


def main():
    #files_to_read = generate_file()
    cur = con.cursor()
    files_to_read = str(input("Enter directory: ")) 
    files_to_read = glob.glob("%s/*.csv" % files_to_read)
    
    heading,data = process_file(files_to_read)
    courses,locations,students =  divide_file(data)
    clean(students,courses,locations)
    sc = summer_conflict(courses,locations)
    cs,sid_conflict = check_student(students,courses)
    sc = sc.union(cs)
    create_tables()
    info = insert_students(students,courses,sid_conflict)
    course_info = insert_course(courses,students,locations,sc)
    insert_location(courses,locations)

    return 

main()




