from bbreporting import BbReporting
import unittest
import py
import pytest_skipping
import re
import time

class BlackboardReports(unittest.TestCase):
	
  aValidTerm = '200909'
  anInvalidTerm = '20090'

  def testLoadConnectionConfigs(self):
    """should successfully import configurations"""
    self.pending() 

  def testConnectToDB(self):
    """successfully connect to the database"""
    self.pending() 

  def testBuildQueryForExpoToolByTerm(self):
    """should create a query to count deployments of LO expo tool"""
    self.pending() 

    bbreporting = BbReporting()
    query = bbreporting.buildQueryForExpoToolByTerm(self.aValidTerm, role)
    self.assertTrue(query.find(self.aValidTerm) > -1)
    #self.assertTrue(query.find("role='P'") > -1)

  def testBuildQueryForNonCreditIccieYtd(self):
    """should create a query to count ICCIE users year-to-date (july 1)"""
    self.pending() 

    bbreporting = BbReporting()
    query = bbreporting.buildQueryForNonCreditIccieYtd(self.aValidTerm)
    self.assertTrue(query.find(self.aValidTerm) > -1)

  def testBuildQueryForInstructorsByTerm(self):
    """should create query to countinstructors by term"""
    role = 'P'
    bbreporting = BbReporting()
    query = bbreporting.buildQueryByTermAndRole(self.aValidTerm, role)
    self.assertTrue(query.find(self.aValidTerm) > -1)
    self.assertTrue(query.find("role='P'") > -1)

  def testBuildQueryForStudentsByTerm(self):
    """should return query to count students by term"""
    role = 'S'
    bbreporting = BbReporting()
    query = bbreporting.buildQueryByTermAndRole(self.aValidTerm, role)
    self.assertTrue(query.find(self.aValidTerm) > -1)
    self.assertTrue(query.find("role='S'") > -1)

  def testBuildQueryForCoursesByTerm(self):
    """should return query to select courses by term"""
    bbreporting = BbReporting()
    query = bbreporting.buildCourseQueryByTerm(self.aValidTerm)
    self.assertTrue(query.find(self.aValidTerm) > -1)
 
  def testCountStudentsByTerm(self):
    """should return a number greater than or equal to 0"""
    bbreporting = BbReporting()
    students = bbreporting.countStudentsByTerm(self.aValidTerm)
    self.assertTrue(students >= 0)
  
  def testCountInstructorsByTerm(self):
    """should return a number greater than or equal to 0"""
    bbreporting = BbReporting()
    instructors = bbreporting.countInstructorsByTerm(self.aValidTerm)
    self.assertTrue(instructors >= 0)
 
  def testCountCoursesByTerm(self):
    """should return a number greater than or equal to 0"""
    bbreporting = BbReporting()
    courses = bbreporting.countCoursesByTerm(self.aValidTerm)
    self.assertTrue(courses >= 0)
  
  def testBuildReportPathByTerm(self):
    """should return a reporting path based on term, report type, and a timestamp"""
    bbreporting = BbReporting()
    reportType = "courses"
    path = bbreporting.buildReportPathByTerm(self.aValidTerm, reportType)
      
    self.assertTrue(path.find("term." + self.aValidTerm + "." + reportType) !=
        -1)

  def testBuildReport(self):
    """should return a report string consisting of a term-based path, a count, and a timestamp"""
    bbreporting = BbReporting()
    reportMethodName = "countCoursesByTerm"
    reportLabel = "courses"
    stamp = int( time.time() )
    path = bbreporting.buildReportPathByTerm(self.aValidTerm, reportLabel)
    report = bbreporting.buildReportByTermAndType(self.aValidTerm, reportLabel,
        reportMethodName, stamp)

    self.assertTrue(path.find("term." + self.aValidTerm + "." + reportLabel) !=
        -1)

    # match '200909.courses 1200 1294768918'
    report_pattern = re.compile ("term\.\d{6}\.\D+\s\d+\s\d{10}")
    self.assertTrue(report_pattern.match(report) != None)
  
  def testSendPastReports(self):
    """should prepare and send report data based on passed data"""
    self.pending() 
	
  def testRunReports(self):
    """should run a set of reports"""
    self.pending() 

  def testBuildTermList(self):
    """should build a list of terms from a specified date until the present"""
    self.pending() 
    
  def testGetCurrentTerm(self):
    """should retrieve the current term code, based on the date"""
    self.pending() 

  def pending(self, message = "pending"):
    py.test.skip(message)
    
if __name__ == "__main__":
    unittest.main()

		
