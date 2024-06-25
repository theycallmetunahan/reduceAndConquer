import csv


def calculate_total():

    with open('salaries.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        total = 0
        for row in readCSV:
            if row[3] == 'Machine Learning Engineer':
                total += int(row[6])
        print(total)
        

def calculate_average_all():
    my_dict = {}
    with open('salaries.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            if (row[6] == 'salary_in_usd'):
                continue
            if row[3] in my_dict:
                my_dict[row[3]].append(int(row[6]))
            else:
                my_dict[row[3]] = [int(row[6])]
        for key in my_dict:
            print(key, sum(my_dict[key])/len(my_dict[key]))
    
def calculate_average_US():
    total = 0
    count = 0
    with open('salaries.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            if (row[6] == 'salary_in_usd'):
                continue
            if row[7] == 'US':
                continue
            total += int(row[6])
            count += 1
        print(total/count)
        
def edit_csv(): # it will only have the first 1000 rows
    with open('salaries.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        with open('salaries_edit.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            count = 0
            for row in readCSV:
                if count == 1000:
                    break
                writer.writerow(row)
                count += 1
            
            
def calculate_average_year(year):
    total = 0
    count = 0
    if (year<2023):
        with open('salaries.csv') as csvfile:
            readCSV = csv.reader(csvfile, delimiter=',')
            for row in readCSV:
                if (row[6] == 'salary_in_usd'):
                    continue
                if int(row[0]) < 2023:
                    total += int(row[6])
                    count += 1
            print(total/count)
            return
    with open('salaries.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            if (row[6] == 'salary_in_usd'):
                continue
            if int(row[0]) == year:
                total += int(row[6])
                count += 1
        print(total/count)
    
    
calculate_average_year(int(2022))
    