from cassandra.cluster import Cluster
import time
import pandas as pd
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
#https://github.com/datastax/python-driver
cluster = Cluster()

session = cluster.connect('wikikeyspace')

class wiki(Model):
     page_name= columns.UUID(primary_key=True)
     view_count = columns.Integer()
     language = columns.Text()
start_time = time.time()     
q = wiki.objects.filter(language='es')     
#s = q.filter(page_name__contains='Obama').allow_filtering()
query_test=q.get()
print query_test
#select "page_name", "view_count" from t20160101 limit 20;






#rows = session.execute('select "page_name", "view_count", "language" from t20160101')



#def pandas_factory(colnames, rows):
 #   return pd.DataFrame(rows, columns=colnames)

#session.row_factory = pandas_factory
#session.default_fetch_size = None

#query = "select page_name, view_count from t20160101"
#rslt = session.execute(query, timeout=None)
#df = rslt._current_rows
#df = pd.DataFrame(rows[0])
#d=rows[0]
#print d

#print "the above is d"

#d2={}
#print type(rows)
#d2_list=[]
#for line in rows:
    #print line.page_name, line.view_count
     #if line.language=="en":
     #   list_add=[line.page_name, line.view_count]
        #d2.update({line.page_name:line.view_count})
        #d2[k] = v
      #  d2_list.append(list_add)
    # else:
       # pass
        
#print "Length : %d" % len (d2_list)        
print "--- %s seconds ---" % (time.time() - start_time)
#for key, value in d2.iteritems():   
    #print key, value     