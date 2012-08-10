import sys
from socket import socket
import re
from datetime import datetime
import time
import local_settings as local_settings
from statsd_client import Statsd

class ReportRunner:

  def get_term(self, year = None, month = None):
    """Generate a 6-digit term code.
    
    Takes a year and a month as arguments.  If none are given, a term
    code is generated based on the current date.
    
    Keyword arguments:
    year -- the year, in YYYY format  (default None)
    month -- the month in MM format (default None)
    """
    if year is None or month is None:
      now = datetime.now()
      year = now.year
      month = now.month
    else:
      month = int(month)

    if month in range(1, 6):
      return str(year) + "01"
    elif month in range(6, 9):
      return str(year) + "06"
    elif month in range(9, 13):
      return str(year) + "09"

  def active_terms(self, current_term = None):
    """Generate a list of 5 active terms.

    Takes a term code as argument.  If none is given, the current term is
    assumed. 

    Returns an array of term codes, representing three terms in 
    the past, the current term, and the next term.

    $ python report_runner.py active_terms 201206
    ['201209', '201206', '201201', '201109', '201106']

    Keyword arguments:
    current_term -- The current term to start from (default None).
    """
    if current_term is None:
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
    """Calculate the next term code.

    Determine what one code in the future is."""
    

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
    """Calculate the previous term code.

    """

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
    """Build a query string by calling a query method and a term.

    Dynamically call a method containing the query string, sending the term
    along to specify a de facto time period.

    Return an associative array containing results for each of 
    the surrounding terms.
    """
    if current_term is None:
      current_term = self.get_term()

    active_terms = self.active_terms(current_term)
    queries = dict()
    
    for term in active_terms:
      query = getattr(self, query_method) (term)
          
      queries[term] = query

    return queries
  
  def active_student_enrollments(self, term):
    """Return query string for active student enrollments.
    
    Select distinct students from the activity tracking table, 
    who have entered a course whose label matches the specified 
    "term" at least once.
    """
    query = """select count(distinct activity_accumulator.user_pk1) as active_users from activity_accumulator, course_main, course_users where activity_accumulator.course_pk1 = course_main.pk1 and course_users.crsmain_pk1=course_main.pk1 and course_main.course_id like '""" + term + """%' and course_users.role='S'"""

    return query

  def active_instructors(self, term):
    """Return query string for active instructor enrollments.
    
    Select distinct instructors from the activity tracking table, 
    who have entered a course whose label matches the specified 
    "term" at least once.
    """
    query = """select count(distinct(users.pk1)) from course_main,course_users,users, activity_accumulator where course_users.crsmain_pk1=course_main.pk1 and course_main.pk1=activity_accumulator.course_pk1 and course_main.course_id like '""" + term + """%' and course_users.users_pk1=users.pk1 and course_users.role='P'"""

    return query

  def local_users(self, term):
    """Return query string for locally created users.
    
    Select distinct instructors from the activity tracking table, 
    who have entered a course whose label matches the specified 
    "term" at least once.
    """
    query = """select count(distinct(users.pk1)) from users where users.data_src_pk1=2"""

    return query

  def active_courses(self, term):
    """Return query string for active courses.

    Select courses with a label matching supplied term, 
    which have had at least one student enter them at some point."""

    query = """select count(distinct course_main.course_id) from activity_accumulator, course_main, course_users where activity_accumulator.course_pk1 = course_main.pk1 and course_users.crsmain_pk1=course_main.pk1 and course_main.course_id like '""" + term + """%' and course_users.role='S'"""

    return query


  def run_active_queries(self, query_method, current_term=None):
    """Return results of active course queries.

    Send queries to database, store the results in an associative array, and
    return that array.
    """
    queries = self.build_active_queries(query_method, current_term)
    reports = dict()
    for term,query in queries.items():
      reports[term] = self.send_query(query)
    return reports

  def mysql_connection(self):
    """Return a MySQL connection."""
    import mysql.connector
    mysql_host = local_settings.MYSQL['HOST']
    mysql_user = local_settings.MYSQL['USER']
    mysql_pass = local_settings.MYSQL['PASS']
    mysql_database = local_settings.MYSQL['DATABASE']
    connection = mysql.connector.connect(host=mysql_host,database=mysql_database,user=mysql_user,password=mysql_pass)
    return connection

  def oracle_connection(self):
    """Return an oracle connection handle."""
    import cx_Oracle
    oracle_host = local_settings.DATABASE['HOST']
    oracle_user = local_settings.DATABASE['USER']
    oracle_pass = local_settings.DATABASE['PASS']
    connection_string = oracle_user + "/" + oracle_pass + "@" + oracle_host
    return cx_Oracle.connect(connection_string)

  def send_query(self, query):
    """Send query oracle, and return result."""
    connection = self.oracle_connection()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()[0][0]
    cursor.close()
    connection.close
    return result

  def run_report(self, query_method, current_term=None, report_prefix=None):
    """Build, send, and report a single query"""
    if current_term is None:
	current_term = self.get_term()
    if report_prefix is None:
	report_prefix = query_method
    prefixed_label = str(report_prefix) + "." + current_term
    query = getattr(self, query_method) (current_term)
    report = self.send_query(query)
    self.send_report(prefixed_label, report)
    return report


  def run_reports(self, query_method, report_prefix=None):
    """Build, send, and report a set of term queries"""
    if report_prefix is None:
	report_prefix = query_method

    reports = self.run_active_queries(query_method) 
    for label, report in reports.items():
      prefixed_label = str(report_prefix) + "." + label
      self.send_report(prefixed_label, report)
    return reports
    
    
  def send_report(self, report_label, report, delivery = "mysql", stamp = None):
    """Send report data to specified aggregator."""
    if delivery is "statsd":
      return "Statsd: " + report_label + " " +  str(report)
    elif delivery is "carbon":
      if stamp is None:
        stamp = int (time.time())
      report_string = report_label + " " + str(report) + " " + str(stamp) + "\n"
      self.send_to_carbon(report_string)
      return "Carbon: " + report_string
    else:
      self.send_to_mysql(report_label, report)

  def send_to_mysql(self, report_label, report):
	connection = self.mysql_connection()
	cursor = connection.cursor()
	cursor.execute("insert into reports (label, report) values('" + str(report_label) + "','" + str(report) + "')")
	connection.commit()
	cursor.close()
	connection.close()

  def send_to_statsd(self, report_label, report):
    """Send data to aggregator via statsd."""
    Statsd.update_stats(report_label, report)

  def send_to_carbon(self, report):
    """Send data to aggregator directly via carbon."""
    carbon_host = local_settings.CARBON['HOST']
    carbon_port = int(local_settings.CARBON['PORT'])
    sock = socket()
    try:
      sock.connect( (carbon_host, carbon_port) )
    except:
      print "Couldn't connect to %s on port %d, is carbon-agent running?" % (carbon_host, carbon_port)
      sys.exit(1)
    sock.send(report)

if __name__ == '__main__':
  import argparse
  import inspect
  parser = argparse.ArgumentParser()
  subparsers = parser.add_subparsers()
  report_runner = ReportRunner()
  for name in dir(report_runner):
    if not name.startswith('_'):
      attr = getattr(report_runner, name)
      if callable(attr):
        sig = inspect.formatargspec(inspect.getargspec(attr))
        doc = (attr.__doc__ or str(sig)).strip().splitlines()[0]
        subparser = subparsers.add_parser(name, help=doc)
        subparser.add_argument('args', nargs=argparse.REMAINDER)
        subparser.set_defaults(func=getattr(report_runner, name))

  args = parser.parse_args(sys.argv[1:])      

  print args.func(*args.args)

