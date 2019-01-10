import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import copy
from collections import defaultdict
from dynamodb import DynamoDB

df = pd.read_excel('dpa_rankings.xlsx', sheet_name="dpa_east_1")
row_headers = (df.keys())

final_list = []
dy_obj = DynamoDB("us-west-2")


def kill_char(string, n):  # n = position of which character you want to remove
    begin = string[:n]  # from beginning to n (n not included)
    end = string[n + 1:]  # n+1 through end of string
    return begin + end


for row in row_headers:
    temp = []
    fields = df[row]
    for val in fields:
        if (type(val)) is int:
            continue
        else:
            # 1 (NC001)
            if (len(val)) == 7:
                temp.append(None)
            else:
                if len(val) == 9:
                    data = kill_char(val, 2)
                    data1 = kill_char(data, len(data) - 1)
                    data2 = data1.split(' ')
                    temp.append(data2[1])
                if len(val) == 10:
                    data = kill_char(val, 3)
                    data1 = kill_char(data, len(data) - 1)
                    data2 = data1.split(' ')
                    temp.append(data2[1])

    final_list.append(copy.copy(temp))
    del temp[:]

# got the data into this list
counter = 0
new_data_dict = defaultdict(list)
new_data_dict["Index"] = 3
for val in final_list:
    index = 0
    if len(val) > 0:
        for item in val:
            if item is not None:
                new_data_dict[str(index)].append(item)
                index = index + 1
            else:
                index = index + 1

# print(new_data_dict)
query_item = {"Index": 3}
print(dy_obj.get_item("Test", query_item))
