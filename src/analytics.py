import numpy as np
df = spark.sql("FROM jason_scott_ram_project.analytic_table SELECT puma, commuters, crashes, complaints, name")
df.show()
rdd = df.rdd

def normal(x):
	return [int(x[0]), x[1], int(x[2]), int(x[3]), x[4]]

rdd1 = rdd.map(normal)
rdd2 = rdd1.sortBy(lambda x: x[2])


def normal1(x):
	return x[0],x[1]

table = rdd2.collect()
data = np.array([[x[1],x[2],x[3]] for x in table])
correlation_matrix = np.corrcoef(data[:,1],data[:,2])
puma_name = [x[0] for x in table]

new_col = sc.textFile("new_col.txt")
newcol = new_col.map(lambda x: x.split("\t")).map(lambda y : [int(y[0]),int(y[1])])
mergecol = newcol.filter(lambda x: x[0] in puma_name)

table2 = mergecol.collect()

for i in range(55):
     for j in range(55):
 		      if table2[i][0]==table[j][0]:
	        		  table[j].append(table2[i][1])

table = [[x[0],x[1],x[5],x[1] + x[5],x[2],x[3],x[4]] for x in table]


ntable = np.array([[x[1],x[2],x[3],x[4],x[5]] for x in table])

np.corrcoef(ntable[:,2],ntable[:,3])
np.corrcoef(ntable[:,1],ntable[:,3])
np.corrcoef(ntable[:,0],ntable[:,3])
np.corrcoef(ntable[:,2],ntable[:,4])




table_man = []

for i in range(55):
	if "Manhattan" in table[i][-1]:
		table_man.append(table[i])



man_crash = 0 
man_com = 0
man_r = 0 
man_p = 0 

for i in range(len(table_man)):
	man_crash += table_man[i][4]
	man_com += table_man[i][5]
	man_r += table_man[i][2]
	man_p += table_man[i][1]
	

man_crash
man_r
man_p
man_com


table_brook = []

for i in range(55):
	if "Brooklyn" in table[i][-1]:
		table_brook.append(table[i])



brook_crash = 0 
brook_com = 0
brook_r = 0 
brook_p = 0 

for i in range(len(table_man)):
	brook_crash += table_brook[i][4]
	brook_com += table_brook[i][5]
	brook_r += table_brook[i][2]
	brook_p += table_brook[i][1]
	

brook_crash
brook_r
brook_p
brook_com



table_bronx = []

for i in range(55):
	if "Bronx" in table[i][-1]:
		table_bronx.append(table[i])



bronx_crash = 0 
bronx_com = 0
bronx_r = 0 
bronx_p = 0 

for i in range(len(table_man)):
	bronx_crash += table_bronx[i][4]
	bronx_com += table_bronx[i][5]
	bronx_r += table_bronx[i][2]
	bronx_p += table_bronx[i][1]
	

bronx_crash
bronx_r
bronx_p
bronx_com

table_queens = []

for i in range(55):
	if "Queens" in table[i][-1]:
		table_queens.append(table[i])



queens_crash = 0 
queens_com = 0
queens_r = 0 
queens_p = 0 

for i in range(len(table_man)):
	queens_crash += table_queens[i][4]
	queens_com += table_queens[i][5]
	queens_r += table_queens[i][2]
	queens_p += table_queens[i][1]
	

queens_crash
queens_r
queens_p
queens_com



