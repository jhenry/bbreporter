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
    def mock_send_query(self, query):
      return 1001
    send_query = ReportRunner.send_query
    try:
      ReportRunner.send_query = mock_send_query
      report_runner = ReportRunner()
      reports = report_runner.run_active_course_queries("201201")
      self.assertTrue(reports["201201"] > 0)
    finally:
      ReportRunner.send_query = send_query

  def test_report_active_courses_to_carbon(self):
    import time
    now = int(time.time())
    def mock_send_to_carbon(self, report):
      return "courses.active.201201 1001"
    send_to_carbon = ReportRunner.send_to_carbon
    try:
      ReportRunner.send_to_carbon = mock_send_to_carbon
      report_runner = ReportRunner()
      reports = report_runner.send_report("courses.active.201201", "1001",
      "carbon", now)
      self.assertIn("courses.active.201201 1001 " + str(now), reports)
    finally:
      ReportRunner.send_to_carbon = send_to_carbon        

  def test_report_active_courses_to_statsd(self):
    def mock_send_to_statsd(self, report):
      return "courses.active.201201 1001"
    send_to_statsd = ReportRunner.send_to_statsd
    try:
      ReportRunner.send_to_statsd = mock_send_to_statsd
      report_runner = ReportRunner()
      reports = report_runner.send_report("courses.active.201201", "1001")
      self.assertIn("courses.active.201201 1001", reports)
    finally:
      ReportRunner.send_to_statsd = send_to_statsd        


   
if __name__ == "__main__":
    unittest.main()

		
