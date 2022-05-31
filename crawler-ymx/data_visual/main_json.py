import kindle_pyspark as spark
import json


#评论总条数
total = int(spark.total_count().iloc[0,0])
#商品总数
pro_total = int(spark.total_count().iloc[0,1])
#根据评论等级统计人数
overall=spark.overall_count().to_dict('records')

#根据商品统计人数
product=spark.product_count().to_dict('records')

#根据评论时间统计人数
date=spark.date_count()["year"].to_list()
#按时间统计评论人数
cnt=spark.date_count()["sum_total"].to_list()
#按时间统计商品数
pro_cnt=spark.date_count()["sum_pro"].to_list()

#统计评论字数
words_total =int(spark.word_count()["total"])
words_a =int(spark.word_count()["a"])
words_b =int(spark.word_count()["b"])
words_c =int(spark.word_count()["c"])
words_d =int(spark.word_count()["d"])
words_e =int(spark.word_count()["e"])

#根据helpful统计帮助多少人购买
helpful=spark.helpful_count().to_dict('records')

#根据评论生成词云
word_cloud=spark.word_cloud().to_dict('records')


# 获取json里面数据
def get_json_data():
    # 使用只读模型，读取json模板，并定义名称为f
    with open('./kindle-template.json', 'rb') as f:  
        # 加载json文件中的内容给params
        params = json.load(f)  
        #评论总数
        params["counter"]["value"] = total

        #商品总数
        params["counter2"]["value"] = pro_total

        #将echart1_data修改为spark计算结果
        params["echart1_data"]["data"] = overall

        #将echart2_data修改为spark计算结果
        params["echart2_data"]["data"] = product

        #echart3_data
        params["echart3_data"]["data"] = word_cloud

        #echart4_data
        params["echart4_data"]["data"][0]["value"] = cnt
        params["echart4_data"]["data"][1]["value"] = pro_cnt
        params["echart4_data"]["xAxis"] = date  

        #echart5_data
        params["echart5_data"]["data"] = helpful

        #echart6_data
        params["echart6_data"]["data"][0]["value"] = words_a
        params["echart6_data"]["data"][1]["value"] = words_b
        params["echart6_data"]["data"][2]["value"] = words_c
        params["echart6_data"]["data"][3]["value"] = words_d
        params["echart6_data"]["data"][4]["value"] = words_e
        params["echart6_data"]["data"][0]["value2"] = words_total - words_a
        params["echart6_data"]["data"][1]["value2"] = words_total - words_b
        params["echart6_data"]["data"][2]["value2"] = words_total - words_c
        params["echart6_data"]["data"][3]["value2"] = words_total - words_d
        params["echart6_data"]["data"][4]["value2"] = words_total - words_e 
    f.close()  # 关闭json读模式
    return params  # 返回修改后的内容
 
 
# 写入json文件# 使用写模式，名称定义为r
def write_json_data(params):
    with open('./kindle.json', 'w',encoding="utf-8") as r:
        # 将params写入名称为r的文件中
        json.dump(params, r,ensure_ascii=False)
    # 关闭json写模式
    r.close()
 
 
# 调用两个函数，更新内容
the_revised_dict = get_json_data()
write_json_data(the_revised_dict)


