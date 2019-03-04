import csv

file_name = "./output/part-r-00000"
topk = [100, 300, 1000]

def cal_acc(k):
    with open(file_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='\t')
        line_count = 0
        count = 0.0
        for row in csv_reader:
            line_count += 1
            if int(row[1]) <= k:
                count += 1
            if line_count >= k:
                break
    return count/k

for k in topk:
    print("topk = {}, recall = {:.4f}".format(k, cal_acc(k)))
