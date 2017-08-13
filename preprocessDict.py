import os
import time
import sys
import os.path
import gzip
import shelve
from collections import defaultdict

if len(sys.argv) != 2:
        print "\n\tusage: sudo python preprocess.py <dir name>\n"
        print "NOTE: does not process recursively (for now)"
        sys.exit()

directory = sys.argv[1]
print "Processing:\t%s\n" % directory

dictFnamesByDate = defaultdict(list)
for filename in os.listdir( directory):
    if "pageviews" in filename and filename.endswith(".gz"):
        #print filename
        # filenames look like: pageviews-20150501-010000.gz
        dictFnamesByDate[filename.split('-')[1]].append( filename)

os.chdir(directory)

for key in dictFnamesByDate:
    # check to make sure we have all hours?!
    #print len(dictFnamesByDate[key])

    time_date_start = time.time()
    # storage structure is a dictionary of dictionaries:
    #    the outer dictionary has language as its key and the inner dictionary as its value
    #    the inner dictionary has the page as its key and the count as its value
    dictAggViews = defaultdict( lambda: defaultdict(int))

    for filename in dictFnamesByDate[key]:
        print filename
        start_time = time.time()
        #i = 0
        with gzip.open( filename, 'r') as fin:
            for line in fin:
                tokens = line.split()
                try:
                    dictAggViews[tokens[0][0:2]][tokens[1]] += int(tokens[2])
                except Exception as e:
                    pass
                    #print "Exception: ", e
                    #print "line:", tokens


                #print(dictAggViews[tokens[0][0:2]])
                #i += 1
                #if i % 10 == 0: break
        end_time=time.time() - start_time
        print "\tTime to process: %s seconds\n" % str(end_time)

    #print len(dictAggViews.keys())
    #print len(dictAggViews['en'].keys())

    # write back out to disk
    with gzip.open( "aggregateviews-%s.gzAgg" % key, 'w') as fout:
        for lang in dictAggViews:
            for page in dictAggViews[lang]:
                #print(fout.filename)
                #print ( lang + ' ' + page + ' ' + str(dictAggViews[lang][page]))
                fout.write( lang + ' ' + page + ' ' + str(dictAggViews[lang][page]) + '\n')

    # rename the original files
    for filename in dictFnamesByDate[key]:
        os.rename( filename, filename + "_proc")

    end_date_time = time.time() - time_date_start
    print "\n\tTotal time for %s: %d seconds\n" % (key, end_date_time)



