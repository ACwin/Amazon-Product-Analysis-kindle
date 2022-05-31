# data_visual
Data big screen visualization
# Function

Convenience tool, simple structure, large data screen can be realized by directly transferring data

# Install

```
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple flask
```

# Run main_json to output the spark calculation results to a json file for the visualization program to read

```
cd data_visual;
python main_json.py;
python app.py;
```

* Big data visualization exhibition board general template http://127.0.0.1:5000/          

# Online example


# use
1. Edit the SourceData class in data.py (or add a new class, if you add a new class, you need to edit app.py to add routes)
2. Read your data from anywhere, fill in the SourceData class according to the data format of SourceDataDemo
3. Run python app.py to view the effect of data changes
