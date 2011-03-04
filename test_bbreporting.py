import bbreporting
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
    py.test.skip("pending")

  def testConnectToDB(self):
    """successfully connect to the database"""
    py.test.skip("pending")

  def testBuildQueryForExpoTool(self):
    """should create a query to count deployments of LO expo tool"""
    py.test.skip("pending")

  def testBuildQueryForNonCreditIccieYtd(self):
    """should create a query to count ICCIE users year-to-date (july 1)"""
    py.test.skip("pending")

  def testBuildQueryForInstructorsByTerm(self):
    """should create query to countinstructors by term"""
    role = 'P'
    query = bbreporting.buildQueryByTermAndRole(self.aValidTerm, role)
    self.assertTrue(query.find(self.aValidTerm) > -1)
    self.assertTrue(query.find("role='P'") > -1)

  def testBuildQueryForStudentsByTerm(self):
    """should return query to count students by term"""
    role = 'S'
    query = bbreporting.buildQueryByTermAndRole(self.aValidTerm, role)
    self.assertTrue(query.find(self.aValidTerm) > -1)
    self.assertTrue(query.find("role='S'") > -1)

  def testBuildQueryForCoursesByTerm(self):
    """should return query to select courses by term"""
    query = bbreporting.buildCourseQueryByTerm(self.aValidTerm)
    self.assertTrue(query.find(self.aValidTerm) > -1)
 
  def testCountStudentsByTerm(self):
    """should return a number greater than or equal to 0"""
    students = bbreporting.countStudentsByTerm(self.aValidTerm)
    self.assertTrue(students >= 0)
    py.test.skip("pending")
  
  def testCountInstructorsByTerm(self):
    """should return a number greater than or equal to 0"""
    instructors = bbreporting.countInstructorsByTerm(self.aValidTerm)
    self.assertTrue(instructors >= 0)
    py.test.skip("pending")
 
  def testCountCoursesByTerm(self):
    """should return a number greater than or equal to 0"""
    courses = bbreporting.countCoursesByTerm(self.aValidTerm)
    self.assertTrue(courses >= 0)
    py.test.skip("pending")
  
  def testBuildReportPathByTerm(self):
    """should return a reporting path based on term, report type, and a timestamp"""
    reportType = "courses"
    path = bbreporting.buildReportPathByTerm(self.aValidTerm, reportType)
      
    self.assertTrue(path.find("term." + self.aValidTerm + "." + reportType) !=
        -1)

  def testBuildReport(self):
    """should return a report string consisting of a term-based path, a count, and a timestamp"""
    reportType = "courses"
    stamp = int( time.time() )
    path = bbreporting.buildReportPathByTerm(self.aValidTerm, reportType)
    report = bbreporting.buildReportByTermAndType(self.aValidTerm, reportType, stamp)

    self.assertTrue(path.find("term." + self.aValidTerm + "." + reportType) !=
        -1)
    # match '200909.courses 1200 1294768918'
    report_pattern = re.compile ("term\.\d{6}\.\D+\s\d+\s\d{10}")
    self.assertTrue(report_pattern.match(report) != None)
  
  def testSendPastReports(self):
    """should prepare and send report data based on passed data"""
    py.test.skip("pending")
	
  def testRunReports(self):
    """should print a set of reports"""
    py.test.skip("pending")

  def testBuildTermList(self):
    """should build a list of terms from a specified date until the present"""
    py.test.skip("pending")
    
  def testGetCurrentTerm(self):
    """should retrieve the current term code, based on the date"""
    #pending() 
    py.test.skip("pending")

  #def pending(self):
    #py.test.skip("pending")
    
if __name__ == "__main__":
    unittest.main()

		
