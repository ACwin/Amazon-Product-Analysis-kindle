from re import U
from datetime import datetime
from selenium import webdriver
from pyquery import PyQuery as pq
import pandas as pd
import time
from multiprocessing.dummy import Pool

#判断是否为数字
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass
 
    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
    return False

#单页解析亚马逊商品评论区数据
def analysis_one(product_id,pageNumber):

    #url='https://www.amazon.com/Fire-HD-8-tablet/product-reviews/' + product_id + '/ref=cm_cr_arp_d_paging_btm_next_2?ie=UTF8&reviewerType=all_reviews&language=en_US&pageNumber=' + str(pageNumber)
    url="https://www.amazon.com/Kindle-Paperwhite-Signature-Essentials-including/product-reviews/"+product_id+"/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews&language=en_US&pageNumber="+str(pageNumber)
    print("当前解析第："+ str(pageNumber) + '页')
    #浏览器
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    browser = webdriver.Chrome(options=options)
    #browser=webdriver.Chrome()
    browser.get(url=url)

    #使用time 模块的sleep方法，暂停给定的秒数后执行后面的程序
    time.sleep(5)
    source_html=browser.page_source
    #pyquery解析页面
    info_html=pq(source_html)
    info=info_html('.a-section.a-spacing-none.review-views.celwidget')
    infolist=[]
    for data in info.items(".a-section.review.aok-relative"):
        dic={}
        #获取日期字符串
        data_str=data(".a-size-base.a-color-secondary.review-date").text().split("on")[1].lstrip()
        #获取帮助
        help_int = data(".a-size-base.a-color-tertiary.cr-vote-text").text().split(" ")[0].replace(',','')
        dic["asin"] = product_id
        if is_number(help_int):
            dic["helpful"] = [int(help_int),int(help_int)]           
        else:
            dic["helpful"] = [0,0]           
        dic["overall"] = data(".a-link-normal i span.a-icon-alt").text()[:1]
        dic["reviewText"] = data(".a-size-base.review-text.review-text-content").text()
        #data处理
        dic["reviewTime"] = datetime.strftime(datetime.strptime(data_str,'%B %d, %Y'),"%m %d, %Y")
        dic["reviewerID"] = data(".a-section.review.aok-relative").attr("id")
        dic["reviewerName"] = data(".a-profile-name").text()
        dic["summary"] = data(".a-size-base.a-link-normal.review-title.a-color-base.review-title-content.a-text-bold").text()
        dic["unixReviewTime"] = int(time.mktime(time.strptime(data_str, '%B %d, %Y') ) )
        infolist.append(dic)
    #然后就是关闭啦
    browser.quit()
    return infolist

def save(product_id,start,end):
    i=0
    amount=[]
    for j in range(start,end):
        datalist=analysis_one(product_id,j)
        i+=1
        print(len(datalist))
        for k in range(0,len(datalist)):
            amount.append(datalist[k])
        print('第{}页解析完成'.format(i))
        
    data=pd.DataFrame(amount)
    #data.to_csv("kindle_review_" + str(start) + "_" + str(end) + ".csv", mode='a', index=False, header=False, encoding="utf-8")
    data.to_csv("./kindle_review_main.csv", mode='a',header=False, encoding="utf-8")

#解析参数
def job1(z):
    """
    :param z:
    :return:
    """
    return save(z[0], z[1],z[2])


#创建多线程调用服务
def run_job(product_id,review_num):
    #构建线程参数
    data_list = []
    inb =  review_num
    page = int((inb-1)/10+1)
    if page == 0 :
        data_list.append((product_id,1,1))
    else:
        for i in range(1,page):
            print(i)
            data_list.append((product_id, i , i+1))
    # p = 1
    # if page == 0 :
    #     data_list.append((product_id,1,1))
    # else:
    #     num = int((page-1)/5+1)
    #     if(num == 0):
    #         for i in range(0,page):
    #             print(i)
    #             data_list.append((product_id, i , i+1))  
    #     for i in range(0,num):
    #         print(i)
    #         data_list.append((product_id, p , p+5))
    #         p=p+5
    print(data_list)
    # 1.实例化一个线程池对象，线程池中开辟四个线程对象，并行4个线程处理四个阻塞操作
    pool = Pool(5) 
    # 2.将列表中的每一个列表元素（可迭代对象）传递给job1函数（发生阻塞的操作）进行处理
    #调用map方法
    #data_list=[('B07TMJ1R3X',1,6),('B07TMJ1R3X',6,11),('B07TMJ1R3X',11,16),('B07TMJ1R3X',21,26),('B07TMJ1R3X',26,31),]
    pool.map(job1,data_list)    #第一个参数为会发生阻塞的函数，第二个参数相当于形参   
    #map方法结束后关闭池子
    pool.close()
    #主线程等待子线程结束后再结束
    pool.join()
    #print(threading.active_count())
    

#'B07TMJ1R3X'
if __name__ == '__main__':
    time1=time.time()
    #请输入商品id 和参与评论的人数
    run_job('B07L5GDTYY',74)
    time2=time.time()
    print('总共耗时：' + str(time2 - time1) + 's')
    print("程序执行结束.....")


    



