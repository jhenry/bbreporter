import sys
import socket
import re
from datetime import datetime
from ConfigParser import SafeConfigParser
import time
import logging
from statsd_client import Statsd

class ReportRunner:

  def __init__(self, loglevel=None):
      parser = SafeConfigParser()
      parser.readfp(open('environment.cfg'))
      parser.readfp(open(parser.get('DEFAULT','environment')))
      self.configs = parser
      self.setup_logger(loglevel)

  def setup_logger(self, loglevel):
      if loglevel is None:
        loglevel = self.configs.get('logging', 'level')
      
      numeric_level = getattr(logging, loglevel.upper(), None)
      if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
      
      logging.basicConfig(level=numeric_level)

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

  def domain_collections(self): 
    """Return querie(s) for counting domain collections 
 
    Select all domains, then select courses, orgs, and user 
    collections for each domain.""" 
    domains = dict()
    domains_query = """select pk1, batch_uid from domain""" 
    domain_results = self.send_query(domains_query, True) 
    for pk1, batch_uid in domain_results:
      domain = str(batch_uid)
      domains[domain] = dict()
      domains[domain]["datatype"] = "domain_collections"
      domains[domain]["domain_id"] = domain 
      domains[domain]["courses"] = self.domain_collection_queries(pk1, "domain_course_coll")
      domains[domain]["organizations"] = self.domain_collection_queries(pk1, "domain_organization_coll")
      domains[domain]["users"] = self.domain_collection_queries(pk1, "domain_user_coll")
      domains[domain]["enrollments"] = self.domain_enrollments(pk1)
 
    return domains

  def domain_enrollments(self, pk1):
    """Count all enrollments for a specified domain.

    Select category for domain, all courses in that category, 
    and all course_users for each course."""
    enrollment_tally = 0
    category_pks = dict()
    course_pks = dict()
    category_query = """select gateway_categories_pk1 from domain_course_categories where domain_pk1 = """ + str(pk1)
    gateway_categories = self.send_query(category_query, True)
    category_pks = self.flatten_list(gateway_categories)
    for category in category_pks:
        course_pk_query = """select crsmain_pk1 from gateway_course_categories where gatewaycat_pk1 = """ + str(category)
        course_pk_results = self.send_query(course_pk_query, True)
        course_pks = self.flatten_list(course_pk_results)
        for crsmain_pk1 in course_pks:
            enrolled_query = """select count(*) from course_users,users where course_users.users_pk1=users.pk1 and users.row_status=0 and crsmain_pk1=""" + str(crsmain_pk1)
            enrolled_results = self.send_query(enrolled_query)
            enrollment_tally += enrolled_results

    return enrollment_tally

  def domain_collection_queries(self, domain, type):
    """Return counts of courses or orgs."""

    query = """select count(*) from """ + str(type) + """ where domain_pk1=""" + str(domain)
    results = self.send_query(query)
    return results
    
  def flatten_list(self, list):
    return [item for sublist in list for item in sublist] 

  def report_domains(self):
    reports = self.domain_collections()
    for report in reports.items():
      self.send_to_splunk(report[1])
        
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
    import pymysql
    
    mysql_host = self.configs.get('mysql', 'host')
    mysql_user = self.configs.get('mysql', 'user')
    mysql_database = self.configs.get('mysql', 'database')
    mysql_password = self.configs.get('mysql', 'password')

    mysql_connection = pymysql.connect(host=mysql_host, user=mysql_user, passwd=mysql_password, db=mysql_database) 

    return mysql_connection

  def socket_connection(self):
    socket_host = self.configs.get('splunk', 'host')
    socket_port = int(self.configs.get('splunk', 'port'))
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((socket_host, socket_port))
    return s

  def oracle_connection(self):
    """Return an oracle connection handle."""
    import cx_Oracle
    oracle_host = self.configs.get('oracle', 'host')
    oracle_port = self.configs.get('oracle', 'port')
    oracle_sid = self.configs.get('oracle', 'sid')
    oracle_user = self.configs.get('oracle', 'user')    
    oracle_pass = self.configs.get('oracle', 'password')
    oracle_dsn_tns = cx_Oracle.makedsn(oracle_host, oracle_port, oracle_sid)

    return cx_Oracle.connect(oracle_user, oracle_pass, oracle_dsn_tns)

  def send_query(self, query, raw=False):
    """Send query oracle, and return result."""
    connection = self.oracle_connection()
    cursor = connection.cursor()
    cursor.execute(query)
    if not raw:
        result = cursor.fetchall()[0][0]
    else:
        result = cursor.fetchall()
    cursor.close()
    connection.close
    return result

  def term_coded_report_label(self, report_prefix, current_term=None):
    if current_term is None:
      current_term = self.get_term()

    prefixed_label = str(report_prefix) + "." + str(current_term)
    return prefixed_label

  def run_report(self, query_method, current_term=None, report_prefix=None):
    """Build, send, and report a single query"""
    if current_term is None:
      current_term = self.get_term()

    query = getattr(self, query_method) (current_term)
    reports = dict()
    reports["term_code"] = current_term
    result = self.send_query(query)
    if type(result) is dict:
      reports << result
      reports["datatype"] = query_method
    else: 
      reports[query_method] = result 
    self.send_to_splunk(reports)
    return reports

  def format_logs(self, report):
    from time import localtime, gmtime, strftime
    if not "stamp" in report:
      report['stamp'] = time.strftime('%Y-%m-%d %H:%M:%S', gmtime())
    
    formatted_log = str(report['stamp']) + " "
    for key,value in report.items():
      if key is not "stamp":
        formatted_log += key + "=" + str(value) + " "
  
    return formatted_log

  def json_to_log(self, json_url):
    import json
    import urllib2
    
    data = urllib2.urlopen(json_url)
    reports = json.load(data)
    for label, report in reports.items():
      labels = label.split('.')
      term_year = labels[1][:4] 
      term_month = labels[1][4:6]
      td = datetime(year=int(term_year), month=int(term_month), day=1)
      print self.format_logs(label,report[0]['report'],td) 
  
  def send_to_splunk(self, report):
    log = self.format_logs(report)
    logging.info(log)
    self.netcat(log)

  def netcat(self, content):
    """Basic netcat clone."""
    s = self.socket_connection()
    s.sendall(content)
    s.shutdown(socket.SHUT_WR)
    s.close()  

  def send_to_statsd(self, report_label, report):
    """Send data to aggregator via statsd."""
    Statsd.update_stats(report_label, report)

  def send_to_carbon(self, report):
    """Send data to aggregator directly via carbon."""
    carbon_host = self.configs.get('carbon', 'host')
    carbon_port = int(self.configs.get('carbon', 'port'))
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

