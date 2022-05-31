# data_visual
数据大屏可视化

# 功能

便利性工具, 结构简单, 直接传数据就可以实现数据大屏

# 安装

```
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple flask
```

# 运行main_json将spark计算结果输出到json文件供可视化程序读取

```
cd data_visual;
python main_json.py;
python app.py;
```

* 大数据可视化展板通用模板 http://127.0.0.1:5000/          

# 在线示例


# 使用
- 1、编辑 data.py 中的 SourceData 类（或者新增类，新增的话需要编辑 app.py 增加路由）
- 2、从任何地方读取你的数据，按照 SourceDataDemo 的数据格式，填充到 SourceData 类
- 3、运行 python app.py 查看数据变更后的效果
