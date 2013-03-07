from report_runner import ReportRunner
import unittest
import py
import pytest_skipping
import re
import datetime

class TestReportRunner(unittest.TestCase):

  def pending(self, message = "pending"):
    py.test.skip(message)

  def test_loaded_configs(self):
    report_runner = ReportRunner()
    self.assertTrue(report_runner.configs.get('carbon', 'port') == "2003")
    
  def test_specific_term(self):
    report_runner = ReportRunner()
    current_term = report_runner.get_term(year=2012, month=3)
    self.assertTrue(current_term == "201201")

  def test_current_term(self):
    report_runner = ReportRunner()
    current_term = report_runner.get_term()
    self.assertRegexpMatches(current_term, '[0-9]{6}')

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
    queries = report_runner.build_active_queries("active_courses","201201")
    self.assertIn("201201", queries)
    self.assertIn("and course_main.course_id like '201201%'", queries["201201"])

  def test_build_active_student_enrollments_queries(self):
    report_runner = ReportRunner()
    queries = report_runner.build_active_queries("active_student_enrollments","201201")
    self.assertIn("201201", queries)
    self.assertIn("and course_main.course_id like '201201%'", queries["201201"])
    self.assertIn("and course_users.role='S'", queries["201201"])

  def test_run_active_course_queries(self):
    def mock_send_query(self, query):
      return 1001
    send_query = ReportRunner.send_query
    try:
      ReportRunner.send_query = mock_send_query
      report_runner = ReportRunner()
      reports = report_runner.run_active_queries("active_courses", "201201")
      self.assertTrue(reports["201201"] > 0)
    finally:
      ReportRunner.send_query = send_query

  def test_term_coded_report_label(self):
    report_runner = ReportRunner()
    label = "active_courses"
    term_code = "201101"
    joined_label = report_runner.term_coded_report_label(label, term_code)
    print joined_label
    self.assertTrue("active_courses.201101" == joined_label)

  def test_format_logs(self):
    report_runner = ReportRunner()
    report = dict()
    report["stamp"] = "2009-09-01 00:00:00"
    report["active_courses"] = 1000
    report["term_code"] = 201101
    log = report_runner.format_logs(report)
    self.assertIn("2009-09-01 00:00:00 term_code=201101 active_courses=1000", log)
    
   
if __name__ == "__main__":
    unittest.main()

		
