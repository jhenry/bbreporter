import sys
from socket import socket
import re
from datetime import datetime
import time
import cx_Oracle
import local_settings as local_settings
from statsd_client import Statsd

class ReportRunner:

  def get_term(self, year = None, month = None):
    if year is None or month is None:
      now = datetime.datetime.now()
      year = now.year
      month = now.month

    if month in range(1, 6):
      return str(year) + "01"
    elif month in range(6, 9):
      return str(year) + "06"
    elif month in range(9, 13):
      return str(year) + "09"

  def active_terms(self, current_term = "201201"):
    try:
      current_term
    except NameError:
      current_term = self.get_term()

    active_terms = [current_term]

    next_term = self.next_term(current_term)
    active_terms.insert(0, next_term)

    last_term = self.previous_term(current_term)
    active_terms.append(last_term)

    two_terms_ago = self.previous_term(last_term) 
    active_terms.append(two_terms_ago)
    
    three_terms_ago = self.previous_term(two_terms_ago) 
    active_terms.append(three_terms_ago)

    return active_terms

  def next_term(self, current_term):

    term_year = current_term[:4] 
    term_month = current_term[4:6]

    if term_month == "01":
      next_term = term_year + "06"
    elif term_month == "06":
      next_term = term_year + "09"
    elif term_month == "09":
      next_year = int(term_year) + 1
      next_term = str(next_year) + "01"

    return next_term

  def previous_term(self, current_term):

    term_year = current_term[:4] 
    term_month = current_term[4:6]

    if term_month == "01":
      previous_year = int(term_year) - 1
      previous_term = str(previous_year) + "09"
    elif term_month == "06":
      previous_term = term_year + "01"
    elif term_month == "09":
      previous_term = term_year + "06"

    return previous_term

  def build_active_queries(self, query_method, current_term=None):
    if current_term is None:
      current_term = self.current_term()

    active_terms = self.active_terms(current_term)
    queries = dict()
    
    for term in active_terms:
      # query = self.active_courses_query(term)
      query = getattr(self, query_method) (term)
          
      queries[term] = query

    return queries
  
  def active_student_enrollments_query(self, term):
    query = """select count(distinct activity_accumulator.user_pk1) as active_users from activity_accumulator, course_main, course_users where activity_accumulator.course_pk1 = course_main.pk1 and course_users.crsmain_pk1=course_main.pk1 and course_main.course_id like '""" + term + """%' and course_users.role='S';"""

    return query

  def active_courses_query(self, term):
    query = """select count(course_main.course_id) from activity_accumulator, course_main, course_users where activity_accumulator.course_pk1 = course_main.pk1 and course_users.crsmain_pk1=course_main.pk1 and course_main.course_id like '""" + term + """%' and course_users.role='S' group by course_main.course_id;"""

    return query


  def run_active_course_queries(self, current_term):
    queries = self.build_active_queries("active_courses_query", current_term)
    reports = dict()
    for term,query in queries.items():
      reports[term] = self.send_query(query)
    return reports


  def oracle_connection(self):
    oracle_host = local_settings.DATABASE['HOST']
    oracle_user = local_settings.DATABASE['USER']
    oracle_pass = local_settings.DATABASE['PASS']
    connection_string = oracle_user + "/" + oracle_pass + "@" + oracle_host
    return cx_Oracle.connect(connection_string)

  def send_query(self, query):
    connection = self.oracle_connection()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()[0][0]
    cursor.close()
    connection.close
    return result
    
  def send_report(self, report_label, report, delivery = "statsd", stamp = None):
    if delivery is "statsd":
      return "Statsd: " + report_label + " " +  str(report)
    else:
      if stamp is None:
        stamp = int (time.time())
      report_string = report_label + " " + str(report) + " " + str(stamp) + "\n"
      self.send_to_carbon(report_string)
      return "Carbon: " + report_string

  def send_to_statsd(self, report_label, report):
      Statsd.update_stats(report_label, report)

  def send_to_carbon(self, report):
    carbon_host = local_settings.CARBON['HOST']
    carbon_port = int(local_settings.CARBON['PORT'])
    sock = socket()
    try:
      sock.connect( (carbon_host, carbon_port) )
    except:
      print "Couldn't connect to %s on port %d, is carbon-agent running?" % (carbon_host, carbon_port)
      sys.exit(1)
    sock.send(report)

