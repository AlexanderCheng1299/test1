##before run the submit script please type in chmod 777 test.sh
##otherwise, you might get denied for runing the script


from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("COMP5349 A2 Data Loading Example") \
    .getOrCreate()

##define the output path
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--output", help="the output path")
args = parser.parse_args()
output_path = args.output

###Part A: preprocess data set
###
from pyspark.sql import SQLContext
#import the json file here
cuad_data = "s3://aws-logs-454823713276-us-east-1/CUADv1.json"
cuad_init_df = spark.read.json(cuad_data)

from pyspark.sql.functions import explode
cuad_data_df= cuad_init_df.select((explode("data").alias('data')))

#assess the paragraph directly here
cuad_paragraph_df = cuad_data_df.select(explode("data.paragraphs").alias("paragraph"))

#assess the context of contracts
cuad_context_df = cuad_paragraph_df.select("paragraph.context")

cuad_qas_df = cuad_paragraph_df.select("paragraph.qas")

cuad_ans_df = cuad_qas_df.select("qas.answers")

# transfer the dataframe into rdds
cuad_paragraph_rdd = cuad_paragraph_df.rdd

context_rdd = cuad_context_df.rdd

context_rdd1 = cuad_context_df.rdd

qas_rdd = cuad_qas_df.rdd

###Part B: my self define functions
###
def con_split(record,size=4096,step=2048):
  list1 = []
  #record = record.collect()
  for cur in [record]:
    list2 = []
    pot = 0
    while pot in range(len(cur[0])):
      list = []
      if (pot+size) < len(cur[0]):
          list.append(cur[0][pot:pot+size])
      else:
          k = len(cur[0])
          list.append(cur[0][pot:k])  
          list2.append(list)
          break
      pot+=step
      list2.append(list)
    list1.append(list2)
  return list1

def get_question(record):
  list1 = []
  i = 0
  for raw in record:
    for cur in raw[0]:
      list1.append(cur[3])
      if i == 40:
        return list1
      i+=1
  return list1

global counter
counter = 0

def catch_pos(record,size=4096,step=2048):
    global counter
    for cur in [record]:
      list2 = []
      for itr in ans2[counter]:
        list1 = []
        if len(itr)> 0:
              for itr1 in itr:
                pot = 0
                while pot in range(len(cur[0])):
                  if (pot+size) < len(cur[0]):
                    if pot < itr1[0] and itr1[0] < (pot+size):
                      list1.append(cur[0][pot:pot+size])
                  else:
                    k = len(cur[0]) - 1
                    if pot<itr1[0] and itr1[1] < k:
                        list1.append(cur[0][pot:k]) 
                    break
                  pot+=step
        list2.append(list1)
      counter+=1
      return list2



  

def conGet(record,size=4096,step=2048):
  list3 = []
  #record = record.collect()
  for raw in [record]:
      list2 = []
      pot = 0
      cur = raw[0][1][0]
      while pot in range(len(raw[0][0])):
        list4 = []
        list1 = []
        k = 0
        if (pot+size) < len(raw[0][0]):
          for itr in cur[0]:
                if itr[0] > pot & (itr[0]+len(itr[1])) < pot+2048:
                  question = cur[3]
                  k = 1
                  list1.append(question)
          nlist1=list(set(list1))
          for i in range(size):   
            list4.append(raw[0][0][pot+i])
          if k ==1:
            nlist1.append(list4)
        else:
          
          for itr in cur[0]:
                if itr[0]>pot&(itr[0]+len(itr[1])) <len(raw[0][0]):
                  question = cur[3]
                  k = 1
                  list1.append(question)
          nlist1=list(set(list1))
          for m in range(len(raw[0][0])-pot):
            list4.append(raw[0][0][pot+m])
          if k ==1:
            nlist1.append(list4)  
            list2.append(nlist1)
          break
        pot+=step
        if k ==1:
          list2.append(nlist1)
        else:
          list2.append(list4)
      list3.append(list2)
  return list3

def get_ans(record):
  list1 = []
  for raw in record[0]:
    list = []
    if raw[2] != True:
      for cur in raw[0]:        
        start = cur[0]
        end = cur[0]+len(cur[1])
        list.append((start,end))
    list1.append(list) 
  return list1



def find_pos(record):
  i = 0
  list3 = []
  for raw in [record]:
    list2 = []
    cotr = cont[i][0]
    limit = len(cotr)
    for itr in raw:      
      list1 = []
      for cur in itr:
          if cur[0]>2048:
            if cur[0] < 4096:
              st = (1,2)
            elif (limit-cur[0])<2048:
              st = (((cur[0]//2048)+1))
            else:
              bb = cur[0]//2048
              st = (bb,bb+1)
          else:
            st = (1)
          if cur[1]>2048:
            if cur[1] < 4096:
              ed = (1,2)
            elif (limit-cur[1])<2048:
              ed = (((cur[1]//2048)+1))
            else:
              bb = cur[1]//2048
              ed = (bb,bb+1)
          else:
            ed = (1)
          list1.append((st,ed))
      list2.append(list1)
    list3.append(list2)
    i+=1
  return list3

def catch_loc(record):
  i = 0
  for raw in [record]:
    list = []
    for cur in raw:
      cotr = cont[i][0]
      limit = len(cotr)
      list1 = []
      for itr in cur:
        if itr[0]>2048:
            if itr[0] < 4096:
              st = (1,2)
              start1 = itr[0]
              start2 = itr[0]-(st[1]-1)*2048
              start = (start1,start2)
            elif (limit-itr[0])<2048:
              st = (((itr[0]//2048)+1))
              start = itr[0]-(st-1)*2048
            else:
              bb = itr[0]//2048
              st = (bb,bb+1)
              start1 = itr[0]-(st[0]-1)*2048
              start2 = itr[0]-(st[1]-1)*2048
              start = (start1,start2)
        else:
            st = (1)
            start = itr[0]-(st-1)*2048
        if itr[1]>2048:
            if itr[1] < 4096:
              ed = (1,2)
              end1= itr[1]
              end2 = itr[1]-(ed[1]-1)*2048
              end = (end1,end2)
            elif (limit-itr[1])<2048:
              ed = (((itr[1]//2048)+1))
              end = itr[1]-(ed-1)*2048
            else:
              bb = itr[1]//2048
              ed = (bb,bb+1)
              end1 = itr[1]-(ed[0]-1)*2048
              end2 = itr[1]-(ed[1]-1)*2048
              end = (end1,end2)
        else:
            ed = (1)
            end = itr[1]-(ed-1)*2048          
        list1.append((start,end))
      list.append(list1)
    i+=1
    
    return list
                                    

def catch_Q(record):
  list1 = []
  for raw in record[0]:
    list1.append(raw[3]) 
  return list1


def get_real_seq(record):
  i = 0
  for raw in [record]:
    list1 = []
    for cur in raw[0]:
      list2 = []
      if len(cur) != 0:
        for itr in cur:
          if type(itr[0]) == type((0,0)):
            st1 = ct1[0][i][itr[0][0]]
            st2 = ct1[0][i][itr[0][1]]
            st = (st1,st2)
          else:
            st = ct1[0][i][itr[0]]
          if type(itr[1]) == type((0,0)):
            ed1 = ct1[0][i][itr[1][0]]
            ed2 = ct1[0][i][itr[1][1]]
            ed=(ed1,ed2)
          else:
            ed = ct1[0][i][itr[1]]
          tuple1 = (st,ed)
          list2.append(tuple1)
        list1.append(list2)
    i+=1
    return list1

def QNA(record):
  for raw in [record]:
    list2 = []
    i = 0
    for cur in raw:
      list1 = []
      list3 = []
      list1.append(questions[i])
      for itr in cur:
        if type(itr[0]) == type((0,0)):
          if type(itr[1]) == type((0,0)):
            tuple1 = (itr[0][0],itr[1][0])
            tuple2 = (itr[0][1],itr[1][1])
            list3.append(tuple1)
            list3.append(tuple2)
          else:
            tuple1 = (itr[0][0],itr[1])
            tuple2 = (itr[0][1],itr[1])
            list3.append(tuple1)
            list3.append(tuple2)
        else:
          if type(itr[1]) == type((0,0)):
            tuple1 = (itr[0],itr[1][0])
            tuple2 = (itr[0],itr[1][1])
            list3.append(tuple1)
            list3.append(tuple2)
          else:
            list3.append(itr)
      list1.append(list3)
      if i == 41:
        return list2
        list2 = []
      i+=1
      list2.append(list1)
    return list2

global i
i = 0      
def comb_2(record):
  global i
  for raw in [record]:
    k = 0
    list3 = []
    for cur in raw:
        list2 = []
        if len(cur) > 0:
          for j in range(len(qna1[i][k][1])):
                  list1 = []
                  list1.append(cur[j])
                  list1.append(qna1[i][k][0])
                  list1.append(qna1[i][k][1][j])
                  list2.append(list1)
        else:
          list1 = []
          list1.append(qna1[i][k][0])
          list2.append(list1)
        list3.append(list2)
        k+=1
    i+=1 
    return list3

def one_func(record,size=4096,step=2048):
    for row in [record]:
      list2 = []
      for cur in row[0][1]:
        list1 = []
        if len(cur[0])> 0:
              for itr1 in cur[0]:                
                pot = 0
                while pot in range(len(row[0][0])):
                  list3 = []
                  if (pot+size) < len(row[0][0]):
                    if pot < itr1[0] and itr1[0] < (pot+size):
                      list3.append(row[0][0][pot:pot+size])
                      list3.append(cur[3])
                      start = itr1[0]-pot
                      end = start+len(itr1[1])
                      list3.append(start)
                      list3.append(end)
                      list1.append(list3)
                  else:
                    k = len(row[0][0]) - 1
                    if pot<itr1[0] and itr1[0]+len(itr1[1]) < k:
                        list3.append(row[0][0][pot:k])
                        list3.append(cur[3])
                        start = itr1[0]-pot
                        end = start+len(itr1[1])
                        list3.append(start)
                        list3.append(end)
                        list1.append(list3) 
                    break
                  pot+=step
        list2.append(list1)
      return list2    

global list1
list1 = []
for i in range(41):
  list1.append([0,0])

def count_pos(record,size=4096,step=2048):
    global list1
    for row in [record]:
      j = 0
      for cur in row[0][1]:
        if len(cur[0])> 0:
              list1[j][1]+=1
              for itr1 in cur[0]:                
                pot = 0
                while pot in range(len(row[0][0])):
                  if (pot+size) < len(row[0][0]):
                    if pot < itr1[0] and itr1[0] < (pot+size):
                      list1[j][0]+=1
                  else:
                    k = len(row[0][0])
                    if pot<itr1[0] and itr1[0]+len(itr1[1]) < k:
                        list1[j][0]+=1
                    break
                  pot+=step
        j+=1
    return  list1

def imp_neg(record):
  list1 = []
  for itr in record:
    tt = itr[0]//itr[1]
    list1.append(tt)
  return list1

def extract_pos(record):
    for row in [record]:
      list2 = []
      for cur in row[0][1]:
        list1 = []
        if len(cur[0])> 0:
              for itr1 in cur[0]:                
                list2.append((itr1[0],(itr1[0]+len(itr1[1]))))
      return list2

def red_func(record,size=4096,step=2048):
    for row in [record]:
      list2 = []
      pot = 0
      while pot in range(len(row[0][0])):
        list3 = []
        i = 0
        if (pot+size) < len(row[0][0]):
            for cur in row[0][1]:
              if len(cur[0])> 0:
                    for itr1 in cur[0]:                
                      if pot < itr1[0] and itr1[0] < (pot+size):
                        i = 1
        else:
                    k = len(row[0][0]) - 1
                    for cur in row[0][1]:
                      if len(cur[0])> 0:
                            for itr1 in cur[0]:                
                              if pot < itr1[0] and itr1[0] < k:
                                i = 1
                    break
        pot+=step
        if i ==1:
          continue
        else: 
          list3.append(row[0][0][pot:pot+size])
        list2.append(list3)
      return list2  

def gen_neg(record):
  i = 0
  for row in [record]:
    j = 0
    if len(neg_pool1[i]) > 0:
      for cur in row:
        question = question[j]
        if len(cur) > 0:  
          for k in range(len(cur)):
            list1 = []
            cu = len(neg_pool1[i])
            if k < cu:
              list1.append(neg_pool1[i][k][0])
              list1.append(question)
              list1.append(0)
              list1.append(0)
            else:
              list1.append(neg_pool1[i][cu-k][0])
              list1.append(question)
              list1.append(0)
              list1.append(0)
            cur.append(list1)
        j+=1
    else:
      for cur in row:
        if len(cur) > 0:
          if len(cur)%2 == 0:
            for  i in range(len(cur)//2):
              cur[i][2] = 0
              cur[i][3] = 0
          else:
            if len(cur) != 1:
              cur.pop(0)
              for  i in range(len(cur)//2):
                cur[i][2] = 0
                cur[i][3] = 0
    i+=1
    return row

def get_imp(record):
  j = 0
  for row in [record]:
    k = 0
    for cur in row:
      question = questions[k]
      if len(cur) == 0:
        if len(neg_pool1[j])> 0:
          cu = len(neg_pool1[j])
          for i in range(imp_num[k]):
            list1 = []
            if i < cu:
              list1.append(neg_pool1[j][i][0])
              list1.append(question)
              list1.append(0)
              list1.append(0)
            else:
              list1.append(neg_pool1[j][cu-i][0])
              list1.append(question)
              list1.append(0)
              list1.append(0)
            cur.append(list1)
        else:
          cu = len(seq1[j][0])
          for i in range(imp_num[k]):
            list1 = []
            if i < cu:
              list1.append(seq1[j][0][i][0])
              list1.append(question)
              list1.append(0)
              list1.append(0)
            else:
              list1.append(seq1[j][0][cu-i][0])
              list1.append(question)
              list1.append(0)
              list1.append(0)
            cur.append(list1)
      k+=1
    j+=1
    return row

def samp_pre(record):
  for row in [record]:
    list1 = []
    for cur in row:
      for itr in cur:
        list1.append(itr)
    return list1


def trans_samp(record):
  for row in [record]:
    list1 = []
    for cur in row:
      dict1 = {
          'source':cur[0],
          'question':cur[1],
          'answer_start':cur[2],
          'answer_end':cur[3]
      }
      list1.append(dict1)
    return list1

###Part C: process the data
###

##get positive sample here
positive_samp = cuad_paragraph_rdd.map(one_func)

yu = positive_samp.collect()

yu[0]

seq_rdd = context_rdd.map(con_split)

seq1 = seq_rdd.collect()

seq1[1][0][0]

a1 = cuad_paragraph_rdd.map(count_pos).collect()[509]

imp_num = imp_neg(a1)

imp_num

cont = context_rdd.collect()

tk = qas_rdd.collect()

tk

questions = get_question(tk)

questions

ans1 = qas_rdd.map(get_ans)

ans2 = ans1.collect()

pos_seq = context_rdd.map(catch_pos)

ans_loc = ans1.map(catch_loc)

qna = ans_loc.map(QNA)

qna1 = qna.collect()

##generate a pool for negative sequences here
neg_pool = cuad_paragraph_rdd.map(red_func)

neg_pool1= neg_pool.collect()

neg_pool1[1]

##add the possible negative into positive sample rdd here
psb_neg = positive_samp.map(gen_neg)
##add the impossible negative into the rdd here
sample = psb_neg.map(get_imp)

sample_ready = sample.map(samp_pre)

sample_ready.take(1)

##transfer the list into dict here
sample_trans = sample_ready.flatMap(trans_samp)

sample_trans1 = sample_trans.collect()

##generate a json output here, notice that the output json would be 
##settled in the same path as the script, the json includes all the 
##positive, possibe negative and impossible negative samples
import json
with open('1.json', 'w') as f:
  json.dump(sample_trans1,f)
