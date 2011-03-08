import re
import time
import dbconfig
import sys
#import cx_Oracle
from socket import socket

class BbReporting:

  def __init__(self):
    self.CARBON_SERVER = '127.0.0.1'
    self.CARBON_PORT = 2003

  def buildQueryByTermAndRole(self, term, role):
    query = """select distinct(users.email) 
        from course_main,course_users,users, activity_accumulator 
        where course_users.crsmain_pk1=course_main.pk1 
        and course_main.pk1=activity_accumulator.course_pk1 
        and course_users.users_pk1=users.pk1 
        and course_users.row_status='0' 
        and course_users.role='""" + role + """' 
        and course_main.row_status='0' 
        and course_main.course_id LIKE '""" + term + "%'"
    
    return query 

  def buildCourseQueryByTerm(self, term):
    query = """select count(course_main.course_id)
        from activity_accumulator, course_main, course_users
        where activity_accumulator.course_pk1 = course_main.pk1
        and course_users.crsmain_pk1=course_main.pk1
        and course_main.course_id like '""" + term + """%'
        and course_users.role='S'
        group by course_main.course_id"""
    
    return query 

  def countInstructorsByTerm(self, term):
    instructors = 0
    query = self.buildQueryByTermAndRole(term, "P")
    # connect to db and run query
    return instructors

  def countStudentsByTerm(self, term):
    students = 0
    query = self.buildQueryByTermAndRole(term, "S")
    # connect to db and run query
    return students

  def countCoursesByTerm(self, term):
    courses = 0
    query =self.buildCourseQueryByTerm(term)
    # connect to db and run query
    return courses

  def buildReportPathByTerm(self, term, reportType):
    path = "term." + term + "." + reportType 
    return path

  def buildReportByTermAndType(self, term, reportLabel, reportMethod, stamp=""):
    if stamp == "":
        stamp = int( time.time() )
    
    print 'locals: %s' % locals()
    path = self.buildReportPathByTerm(term, reportLabel)
    #result = locals()[reportMethod](term)
    result = getattr(self, str(reportMethod))(term)
    # if reportType == "courses":
    #   result = countCoursesByTerm(term)
    # elif reportType == "instructors":
    #   result = countInstructorsByTerm(term)
    # elif reportType == "students":
    #   result = countStudentsByTerm(term)
    
    report =  path + " "+ str( result ) + " " + str( stamp )
    
    return report

  def runReports():
    reports = []
    
    reports.append(buildReportByTermAndType("200909", "courses"))
    reports.append(buildReportByTermAndType("200909", "students"))
    reports.append(buildReportByTermAndType("200909", "instructors"))
    
    return reports

  def sendReports(verbose = False, delay = 60):
    carbon_socket = getCarbonConnection() 
    while True:
      full_report = '\n'.join(runReports()) + '\n' #all lines must end
      if verbose == True:
        printReports(full_report)
      carbon_socket.sendall(full_report)
      time.sleep(delay)

  def printReports(reports):
      print "\nSending reports\n"
      print '-------------------'
      print reports

  def getCarbonConnection():
    sock = socket()
    try:
      sock.connect( (CARBON_SERVER,CARBON_PORT) )
    except:
      print "Couldn't connect to localhost on port %d, is carbon-agent running?" % CARBON_PORT
      sys.exit(1)
    return sock

  def getOracleConnection():
    connection_string = DBConfig.USER + "/" + DBConfig.PASS + "@" + DBConfig.DATABASE
    return cx_Oracle.connect(connection_string)

  def sendQuery(query):
    connection = getOracleConnection()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()[0][0]
    cursor.close()
    connection.close
    return result

