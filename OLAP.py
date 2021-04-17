#!/usr/bin/env python3
#Vibhu Behl
#V00913786
import argparse
import os
import sys
import csv


#appends list horizontally
def lappend(lis,orig):
    i=0
    for temp in orig:
        try:
            lis[i]=lis[i]+','+str(temp)
        except:
            lis.append(str(temp))
        i=i+1



#sums the category mentioned and counts in simulataeniously
def sum_csv(filename,groupby,indexgb,index_entity,mode):
    sum=0 
    i=0
    c=1
    nonnum=0
    with open(filename, 'r') as csvFile:
        reader = csv.reader(csvFile)

        for row in reader:
            if(i==0):
                i+=1
                continue
            if(nonnum>=100):#error 
                    print(filename +" :more than 100 non-numeric values found in aggregate column "+groupby)
                    exit(7)
            if(row[indexgb]==groupby and mode==1):
                try:
                    sum=sum+float(row[index_entity])
                except:#non numeric value
                    nonnum+=1
                    print(filename+" : "+str(c)+" : can’t compute" +str(sum) +" on non-numeric value "+row[indexgb])
                    continue
                i+=1


            elif(mode==0):#no group by query
                try:
                    sum=sum+float(row[index_entity])
                except:
                    nonnum+=1
                    print(filename+" : "+str(c)+" : can’t compute" +str(sum) +" on non-numeric value "+row[indexgb])
                    pass
                i+=1
            c=c+1

    return (sum)



#counting function
def count_csv(filename,groupby,indexgb,index_entity,mode):
    sum=0 
    i=0
    c=1
    with open(filename, 'r') as csvFile:
        reader = csv.reader(csvFile)

        for row in reader:
            if(i==0):
                i+=1
                continue

            if(row[indexgb]==groupby and mode==1):#group by query
                try:
                    sum=sum+float(row[index_entity])
                except:
                    pass
                i+=1


            elif(mode==0):#no group by query
                try:
                    sum=sum+float(row[index_entity])
                except:
                    pass
                i+=1
            c=c+1

    return (i-1)



#finds min and max the category mentioned.
def minmax(filename,groupby,indexgb,index_entity,mode):
    min=0
    max=0 
    nonnum=0
    i=-1
    c=1
    with open(filename, 'r') as csvFile:
        reader = csv.reader(csvFile)

        for row in reader: 
            if(i==-1):#ignoring header files
                i=0
                continue

            if(nonnum>=100):#error
                    print(filename +" :more than 100 non-numeric values found in aggregate column "+groupby)
                    exit(7)

            if(row[indexgb]==groupby and mode==1):#group by query
                
                try:
                    if(i==0):
                        min=float(row[index_entity])
                        max=float(row[index_entity])
                        i=-99

                    if(min>float(row[index_entity])):
                        min=float(row[index_entity])
                    if(max<float(row[index_entity])):
                        max=float(row[index_entity])
                
                except:
                    nonnum+=1
                    print(filename+" : "+str(c)+" : can’t compute min/max on non-numeric value "+row[indexgb])
                    continue

            elif(mode==0):#no group by query

                try:
                    if(i==0):
                        min=float(row[index_entity])
                        max=float(row[index_entity])
                        i=-99
                    if(min>float(row[index_entity])):
                        min=float(row[index_entity])
                    if(max<float(row[index_entity])):
                        max=float(row[index_entity])
                except:
                    nonnum+=1
                    print(filename+" : "+str(c)+" : can’t compute min/max on non-numeric value "+row[indexgb])
                    continue
            c+=1

    return (min,max)



#dissects input and calls appropriate function
def processor(info):
    filename=info[2]
    mode=0
    i=0
    flagtop=0 #keeps track of whether top is flagged or not
    names=None
    group_value=list()
    group_top=list()
    count_top=dict()
    top=list()
    ans=list()
    name=dict()

    try:
        with open(filename, 'r') as csvFile:
            reader = csv.reader(csvFile)
            for row in reader:
                names=row
                break
    except:
        print("Error : inavlid filename", file=sys.stderr)
        exit(6)
    i=0

    #initializing name
    for temp in names:
        name[temp]=i
        i=i+1

    #group by query
    if(info[3]=='--group-by'):
        mode=1

        try:
            index=name[info[4]]
        except:
            print("Error: "+ filename+"file>:no group-by argument with name"+ info[4], file=sys.stderr)
            exit(9)
        with open(filename, 'r') as csvFile:#finding distinct group by value
            reader = csv.reader(csvFile)
            i=0

            for row in reader:
                if(i==0):
                    i=2
                    continue

                flag=1

                #finding distict values for group by
                for temp in group_value:
                    if(temp==row[index]):#temp not distinct
                        flag=0
                        break
                if(flag==1):
                    group_value.append(row[index]) 
                    i=i+1
                    if(i>=1000):#in case of overflow
                        print("Error "+filename +" : "+info[4]+" has been capped at 1000 values", file=sys.stderr) 
                        break
            group_value.sort()
            lappend(ans,group_value)
    try:
        indexgb=name[info[4]]
    except:
        pass
    

    j=0
    i=0

    #executing info commands
    for temp in info:
        suml=list()
        count=list()
        maxl=list()
        minl=list()
        mean=list()
        #in case of sum,count or mean with group by query
        if((temp=='--sum' or temp=='--count' or temp=='--mean') and mode==1):
            if(temp!='--count'):
                index_entity=name[info[j+1]]

            for temp1 in group_value:#computing relevant operation for each distict value
                countt=count_csv(filename,temp1,indexgb,0,mode)
                count.append(countt)
                if(temp!='--count'):
                    sumt=sum_csv(filename,temp1,indexgb,index_entity,mode)
                    if(temp=='--sum'):
                        suml.append(sumt)
                    elif(temp=='--mean'):
                        mean.append(sumt/countt)

            if(temp=='--sum'):
                lappend(ans,suml)
            if(temp=='--count'):
                lappend(ans,count)
            if(temp=='--mean'):
                lappend(ans,mean)
            
        #in case of sum/count/mean without group query
        if((temp=='--sum' or temp=='--count'or temp=='--mean') and mode==0):
            try:
                index_entity=name[info[j+1]]
            except:
                if(temp=='--count'):
                    index_entity=0
                else:
                    print("Error: " +filename+" :no field with name "+info[j+1])
                    exit(8)

            countt=count_csv(filename,None,0,index_entity,mode)   
            if(temp!='--count'):
                sumt=sum_csv(filename,None,0,index_entity,mode)
                if(temp=='--sum'):
                    suml.append(sumt)
                if(temp=='--mean'):
                    mean.append(sumt/countt)

            if(temp=='--sum'):
                lappend(ans,suml)
            if(temp=='--count'):
                count.append(countt) 
                lappend(ans,count)
            if(temp=='--mean'):
                lappend(ans,mean)

        #find min/max with group by query    
        if((temp=='--min' or temp=='--max') and mode==1):
            index_entity=name[info[j+1]]

            for temp1 in group_value:

                mint,maxt=minmax(filename,temp1,indexgb,index_entity,mode)
                if(temp=='--max'):
                    maxl.append(maxt)
                if(temp=='--min'):
                    minl.append(mint)
 
            if(temp=='--min'):
                lappend(ans,minl)
            if(temp=='--max'):
                lappend(ans,maxl)
            
        #find min/max without group by query 
        if((temp=='--min' or temp=='--max') and mode==0):
            index_entity=name[info[j+1]]
            mint,maxt=minmax(filename,None,0,index_entity,mode) 
            if(temp=='--min'):
                minl.append(mint)
                lappend(ans,minl)
            if(temp=='--max'):
                maxl.append(maxt) 
                lappend(ans,maxl)

        if(temp=='--top'):
            index_top=name[info[j+2]]
            n=info[j+1]   
            with open(filename, 'r') as csvFile:#finding distinct group by value
                reader = csv.reader(csvFile)
                i=0

                for row in reader:
                    if(i==0):
                        i=2
                        continue
                    flag=1
                    for temp in group_top:
                        if(temp==row[index_top]):#temp not distinct
                            flag=0
                            break
                    if(flag==1):
                        group_top.append(row[index_top])
                        i=i+1

                        if(i>=20):
                            print(filename+ " : " +info[j+2]+" has been capped at 20 distinct values", file=sys.stderr)
                            flagtop=1
                            break

                for temp in group_top:
                    countt=count_csv(filename,temp,index_top,index_top,1)
                    count_top[temp]=(countt)

                sorted_x = sorted(count_top.items(), key=lambda kv: kv[1])
                i=0
                it=len(count_top)
                temp_name=''
                temp1=list()
                for a in sorted_x:
                    if(i>=it-int(n)):
                        top.append(a[1])
                        temp_name=temp_name+" "+str(a[0])+" "+str(a[1])
                    i=i+1

                i=len(top)-1  
            temp1.append(temp_name)
            top.reverse()
            lappend(ans,temp1)
        j=j+1

    #insert final lists into output.csv
    with open('output.csv', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=' ',quotechar='|', quoting=csv.QUOTE_MINIMAL)
        str1=''
        i=0
        
        for a in info:
            flag=0

            if(a=='--group-by'):
                str1=str1+info[i+1]
                flag=1
            if(a=='--mean'):
                str1=str1+"mean_"+info[i+1]
                flag=1
            if(a=='--top' and flagtop==0):
                str1=str1+info[i+2]+","+"top_"+info[i+2]
                flag=1
            if(a=='--top' and flagtop==1):
                str1=str1+info[i+2]+","+"top_"+info[i+2]+"_capped"
                flag=1
            if(a=='--count'):
                str1=str1+"count_"
                flag=1
            if(a=='--max'):
                str1=str1+"max_"+info[i+1]
                flag=1
            if(a=='--min'):
                str1=str1+"min_"+info[i+1]
                flag=1
            if(a=='--sum'):
                str1=str1+"sum_"+info[i+1]
                flag=1

            i=i+1
            if(flag==1):
                if(i==len(info)-1):
                    pass
                else:
                    str1=str1+','
        #will display the result in for of csv
        print(str1)
        for temp in ans:
            print(temp)
            spamwriter.writerow(temp)
        str1=''
        i=0


        

def main():
    # deliberately left blank for your implementation - remove this comment and begin
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', nargs = 1, help = "Enter filename", required = True)
    parser.add_argument('--group-by', nargs=1, type = str, default=None)
    parser.add_argument('--min', action = "append")
    parser.add_argument('--max', action = "append")
    parser.add_argument('--mean', action = "append")
    parser.add_argument('--sum', action = "append")
    parser.add_argument('--count', action = 'store_false')
    parser.add_argument('--top', nargs=2, type = str,  default=None)
    args = parser.parse_args()
    info=sys.argv
    processor(info)
    #output()


if __name__ == '__main__':
    main()
