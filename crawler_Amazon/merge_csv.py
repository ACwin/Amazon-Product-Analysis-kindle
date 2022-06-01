import os
import pandas as pd
import glob

#保存文件的路径
os.chdir(r'/Users/acw/Desktop/bigdata/crawler_Amazon/csv')
csv_list = glob.glob('*.csv')
print(u'共发现%s个CSV文件'% len(csv_list))
print(u'正在处理…')

for i in range(0,len(csv_list)): #循环读取同文件夹下的csv文件
    path = str(csv_list[i])
   
    
    fr = open(path,'rb').read()
    with open('kindle_review_main.csv','ab') as f:#将结果保存为result.csv
        f.write(fr)
    print(i)
print(u'合并完毕！')
