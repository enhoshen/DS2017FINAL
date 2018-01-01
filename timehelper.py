from functools import reduce

def str2time ( s , dtype='date'):
    s=s.split(' ')
    if dtype == 'date':
        d=  s[0].split('/')
        return [int(d[2]),int(d[0]),int(d[1])]
    elif dtype == 'time':
        t = s[1].split(':')
        return [int(t[0]),int(t[1])]
def parsedstr2time ( s ):
    s=s[1:-1].split(', ')
    return list(map(int,s))
def timesel_interval ( df ,column,  start , end):
    tempdf = df
    s = reduce( lambda x, y : 100*x+y , start)
    e = reduce( lambda x, y : 100*x+y , end)
    l = len(start)
    tempdf =tempdf[ tempdf[column].apply(lambda x : reduce(lambda a,b: 100*a+b, x[:l] )>= s) ]
    if len(tempdf)==0:
        return
    l = len(end)
    tempdf =tempdf[ tempdf[column].apply(lambda x : reduce(lambda a,b: 100*a+b, x[:l] )<= e) ]
    return tempdf
#def timesel ( df , column, time ):
#    return df [ df[column].apply( lambda x : reduce(lambda a,b: a and b , [x[i]==v for i,v in enumerate(time)]) ) ]

