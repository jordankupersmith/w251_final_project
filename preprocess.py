import os
import sys
import time
import os.path
import gzip
import shelve
from collections import defaultdict

if len(sys.argv) != 2:
        print "\n\tusage: sudo python preprocess.py <dir name>\n"
        print "NOTE: does not process recursively (for now)"
        sys.exit()

directory = sys.argv[1]

dictFnamesByDate = defaultdict(list)
for filename in os.listdir( directory):
    if filename.endswith(".gz"):
        #print filename
        # filenames look like: pageviews-20150501-010000.gz
        dictFnamesByDate[filename.split('-')[1]].append( filename)

for key in dictFnamesByDate:
    print key
    for fn in dictFnamesByDate[key]:
        #print '\t', fn
        pass

shelveDictName = "shelveFile"
os.chdir(directory)

for key in dictFnamesByDate:
    
    #print len(dictFnamesByDate[key])

    # storage structure is a dictionary of dictionaries:
    #    the outer dictionary has language as its key and the inner dictionary as its value
    #    the inner dictionary has the page as its key and the count as its value
    dictAggViews = shelve.open( shelveDictName, writeback=True)
    dictAggViews.clear()

    for filename in dictFnamesByDate[key]:#'20150503']:
        if "pageviews" in filename:
                print '\n', filename
                i = 0
                with gzip.open( filename, 'r') as fin:
                    for line in fin:
                        tokens = line.split()
                        try:
                            dictAggViews.setdefault( tokens[0], defaultdict(int)).setdefault(str([tokens[1]]), 0)
                            #print "dictAggViews[%s][%s]: %s" % (tokens[0], tokens[1], dictAggViews[tokens[0]][tokens[1]])
                            #print "count:", tokens[2]
                            #val = dictAggViews[tokens[0][0:2]][tokens[1]].setdefault(str([tokens[1]]), 0)
                            #print "val1: ", val
                            dictAggViews[tokens[0]][tokens[1]] += int(tokens[2])
                            #print "dictAggViews[%s][%s]: %s" % (tokens[0], tokens[1], dictAggViews[tokens[0]][tokens[1]])

                            #print "val: ", val

                        except Exception as e:
                            pass
                            #print "Exception ", e.message, "\nat i=", i, tokens


                        #print "val2", dictAggViews[tokens[0][0:2]][tokens[1]]
                        i += 1
                        #if i % 10 == 0: break
                        # do this periodically to keep our memory usage under control (at the expense of speed)
                        # without some periodic flushing, we pop up over 6 GB RAM. (the whole reason we aren't using
                        # a standard python dictionary)
                        if i % 1000000 == 0:
                            # write the dictionary back to disk
                            dictAggViews.sync()

    #print len(dictAggViews.keys())
    #print len(dictAggViews['en'].keys())

    # write back out to disk
    with gzip.open( "aggregateviews-%s.gzg" % key, 'w') as fout:
        for lang in dictAggViews:
            for page in dictAggViews[lang]:
                #print(fout.filename)
                print ( lang + ' ' + page + ' ' + str(dictAggViews[lang][page]))
                fout.write( lang + ' ' + page + ' ' + str(dictAggViews[lang][page]))

    # erase the dictionary from disk
    dictAggViews.close()
    os.unlink( shelveDictName)

    # rename the original files
    for filename in dictFnamesByDate[key]:
        os.rename( filename, filename + "_proc")

