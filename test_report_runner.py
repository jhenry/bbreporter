from report_runner import ReportRunner
import unittest
import py
import pytest_skipping
# import mox
import re
import datetime
import cx_Oracle

class TestReportRunner(unittest.TestCase):

  def pending(self, message = "pending"):
    py.test.skip(message)

  def test_current_term(self):
    report_runner = ReportRunner()
    current_term = report_runner.get_term(year=2012, month=3)
    self.assertTrue(current_term == "201201")

  def test_active_terms(self):
    report_runner = ReportRunner()
    terms = report_runner.active_terms("201201")
    self.assertTrue(len(terms) == 5)
    self.assertIn("201206", terms)
    self.assertIn("201201", terms)
    self.assertIn("201109", terms)
    self.assertIn("201106", terms)
    self.assertIn("201101", terms)
  
  def test_previous_term(self):
    report_runner = ReportRunner()
    previous_term = report_runner.previous_term("201201")
    self.assertTrue(previous_term == "201109")

  def test_next_term(self):
    report_runner = ReportRunner()
    next_term = report_runner.next_term("201201")
    self.assertTrue(next_term == "201206")

  def test_build_active_course_queries(self):
    report_runner = ReportRunner()
    queries = report_runner.build_active_course_queries("201201")
    self.assertIn("201201", queries)
    self.assertIn("and course_main.course_id like '201201%'", queries["201201"])

  def test_run_active_course_queries(self):
    report_runner = ReportRunner()
    reports = report_runner.run_active_course_queries("201201")
    self.assertIn("active_courses.201201", reports)

   
if __name__ == "__main__":
    unittest.main()

		
