# Bb Reporting 

A collection of queries and their baggage which have the purpose of
archiving usage data over time for a Blackboard System.

## Test it, Test it good!

Moderate test coverage is aspired to.  Tests can be run with something
like:

`py.test -rvx test_report_runner.py`

## City vs Highway, etc

Your mileage shall be variable with these, and you'll be using these at
your own risk. 

UMBC has some good looking tools available for this, and are likely to have a
more robust set of queries available.

You might also be interested in the sort-of-open Project Astro building
block.  

## Install & Configure

This is meant to feed into graphite, but things should fit just fine
into other tools without a too much heavy lifting. Basically, as long as you
can send something a label, a number, and a timestamp, this should work.


If you're using Graphite, you may be aware that it is great for reporting continuous streams of data. It gets a little more cumbersome when you want it to store and present periodic data (say, sending it one point a day). You'll want to be wary of how it's storing and compressing things.

For example, since we're only collecting periodic samples of course enrollment counts on, say, a daily basis, you don't want your data points dropped just because there aren't enough of them.  You also may not want those points averaged over time, since there are so few of them.

Here's how you might set up your storage-schema.conf and
aggregation-schema.conf files to store incoming data with a high rate of
retention and precision:

```
[everything_1min_1day]
priority = 100
pattern = .*
retentions = 10s:10y


[default]
pattern = .*
xFilesFactor = 0.0
```

There are methods included to support sending via statsd or directly to carbon.  The below articles and issues do a good job of explaining how graphite and statsd collect your data and what they do with it.

* https://github.com/etsy/statsd/issues/32#issuecomment-1879427 
* https://github.com/etsy/statsd/issues/22#issuecomment-2549988 
* http://obfuscurity.com/2012/04/Unhelpful-Graphite-Tip-9

## Usage

This can be run as a module or directly from the command prompt.  To use in a python application, you might do something like this: 

```
>>> from report_runner import ReportRunner
>>> rr = ReportRunner()
>>> rr.get_term()
'201206'
>>> rr.active_terms()
['201209', '201206', '201201', '201109', '201106']
>>> rr.build_active_queries("active_courses_query", "201206")
{'201106': "select
count(distinct course_main.course_id) from activity_accumulator,
course_main, course_users where activity_accumulator.course_pk1 =
course_main.pk1 and course_users.crsmain_pk1=course_main.pk1 and
course_main.course_id like '201106%' and course_users.role
...
```

Similarly, via prompt:

```
$ python report_runner.py next_term 201206
201209
$ python report_runner.py previous_term 201206
201201
$ python report_runner.py get_term 
201206
$ python report_runner.py get_term 2011 10
201109
$ python report_runner.py active_terms
['201209', '201206', '201201', '201109', '201106']
$ python report_runner.py active_terms 201109
['201201', '201109', '201106', '201101', '201009']
$ python report_runner.py build_active_queries active_courses_query 201209
{'201201': "select count(distinct course_main.course_id) from
activity_accumulat
...
```




