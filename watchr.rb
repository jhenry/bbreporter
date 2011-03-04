watch('.*\.py') { |md| system("py.test -vrxs test_bbreporting.py") }
