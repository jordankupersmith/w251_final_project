import os
import time
import sys
import io
import gzip
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
        
        with gzip.open( filename, 'r') as fin:
            # BufferedReader is much faster than reading straight from the file
            f = io.BufferedReader(fin)
            for line in f:
                tokens = line.split()
                try:
                    # increment the count
                    dictAggViews[tokens[0][0:2]][tokens[1]] += int(tokens[2])
                except Exception as e:
                    # there are poorly formatted rows here and there, just skip
                    pass
        
        end_time=time.time() - start_time
        print "Time to process: %s seconds" % str(end_time)
    
    # write back out to disk - a chunk at a time for performance
    zip_start_time = time.time()
    with gzip.open( "aggregateviews-%s.gz" % key, 'w') as fout:
        for lang in dictAggViews:
            chunk = []
            chunkIter = 0
            chunkSize = 5000 # tested with a few values, maybe can be tuned further
            for page in dictAggViews[lang]:
                chunk.extend( (str(lang + ' ' + page + ' ' + str(dictAggViews[lang][page])), ))
                chunkIter += 1
                if chunkIter > chunkSize:
                    write( '\n'.join( chunk))
                    chunkIter = 0
                    chunk = []
            # write out the last chunk
            if chunkIter > 0:
                fout.write( '\n'.join( chunk))
                chunkIter = 0
                chunk = []
    print "\tTime to zip: %s seconds\n" % str(time.time() - zip_start_time)
    
    # rename the original files
    for filename in dictFnamesByDate[key]:
        os.rename( filename, filename + "_proc")

    end_date_time = time.time() - time_date_start
    print "\tTotal time for %s: %d seconds\n\n" % (key, end_date_time)



