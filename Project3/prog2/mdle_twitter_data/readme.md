# Twitter data from 2020 and 2021
## MDLE Assignment 3

### **Structure of data**
- The data is separated into two different files fore 2020 and 2021. 
- Each line contains a timestamps and hashtags.
- The data is already sorted by timestamp. 

### **Counts**
```
2020.txt : 1609074 <br>
2021.txt : 1281091 <br>
total    : 2890165
```

### **Reading the data**
To load the data as a python object you can use the following block of code.

```python
with open(filepath) as file:
    data = file.read().splitlines()
    data = [item.split(" ", 1) for item in data]
    data = [tuple((item[0], eval(item[1]))) for item in data]
```

This loads the data as a list of tuples with timestamp, list of hashtags.
```
[
 ('2021-01-01T00:28:53.000Z', ['Noticias']),
 ('2021-01-01T00:30:00.000Z', ['COVID19']),
 ('2021-01-01T00:30:14.000Z', ['asksnk', 'HappyNewYear2021']),
 ('2021-01-01T00:31:10.000Z', ['pandemic', 'covid_19', 'COVIDFest']),
 ('2021-01-01T00:31:16.000Z', ['covid19', 'vacina', 'jacare'])
]
```