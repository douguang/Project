#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  Pandas的学习
@software: PyCharm 
@file: demo_pandas.py 
@time: 17/9/26 下午5:53 
"""
'''
Pandas是python的一个数据分析包，pandas为时间序列提供了很好的支持。

开发Pandas时提出的要求：
    1.具备按轴自动或显示数据对其功能的数据结构
    2.集成时间序列功能
    3.既能处理时间序列数据也能处理非事件序列数据的数据结构
    4.数学运算和约简（比如对某个轴求和）可以根据不同的元数据（轴编号）执行
    5.灵活处理确缺失数据
    6.合并及其他出现在常见数据库（例如基于SQL的）关系型运算


数据结构 Series:
    1.Series是一种类似于一维数组的对象，它由一组数据（各种Numpy数据类型）以及一组与之相关的数据标签（即索引）组成。
    2.Series的字符串表现形式为：索引在左边，值在右边
    3.创建
    4.读写
    5.运算
    例子：
        introduction_to_pandas_data_structure/series.py

'''
from pandas import Series

print '用数组生成Series'
obj = Series([4, 7, -5, 3])
print obj
print obj.values
print obj.index
print

print '指定Series的index'
obj2 = Series([4, 7, -5, 3], index = ['d', 'b', 'a', 'c'])
print obj2
print obj2.index
print obj2['a']
obj2['d'] = 6
print obj2[['c', 'a', 'd']]
print obj2[obj2 > 0]  # 找出大于0的元素
print 'b' in obj2 # 判断索引是否存在
print 'e' in obj2
print

print '使用字典生成Series'
sdata = {'Ohio':45000, 'Texas':71000, 'Oregon':16000, 'Utah':5000}
obj3 = Series(sdata)
print obj3
print

print '使用字典生成Series，并额外指定index，不匹配部分为NaN。'
states = ['California', 'Ohio', 'Oregon', 'Texas']
obj4 = Series(sdata, index = states)
print obj4
print

print 'Series相加，相同索引部分相加。'
print obj3 + obj4
print

print '指定Series及其索引的名字'
obj4.name = 'population'
obj4.index.name = 'state'
print obj4
print

print '替换index'
obj.index = ['Bob', 'Steve', 'Jeff', 'Ryan']
print obj

'''
数据结构 DataFrame
    DateFrame是一个表格型的数据结构，它还有一组有序的列，每列可以是不同的值类型（数值、字符串、布尔值）
    DataFrame既有行索引也有列索引，它可以被看做由Series组成的字典（共用同一索引）
    
        
    可以输入给DataFrame的构造器的数据：
        二维ndarry:数据矩阵，还可以传入行标签和列标签。
        有数组、列表或者元组组成的字典：每个序列都会变成DataFrame的一列，所有序列的长度必须相同。
        Numpy的结构化/记录数组：类似于'由数组组成的字典'
        由Series组成的字典：每个Series会组成一列，如果没有显示指定索引，则各Series的索引会被合并成结果的行索引
        由字典组成的字典：各内层字典会成为一列。键会被合并成结果的索引，跟'由Series组成的字典'的情况一样
        字典或Series的列表：各项将会成为DataFrame的一行。字典键或Series索引的并集将会成为DataFrame的列标
        由列表或元组组成的列表：类似于'二维ndarry'
        另一个DataFrame：该DataFrame的索引将会被沿用，除非显示指定了其他的索引。
        Numpy的MaskedArray：类似于'二维ndarry'的情况，只是掩码值在结果DataFrame会编程NA/缺失值
        
    创建
    读写
    例子：
        introduction_to_pandas_data_structures/dataframe.py

'''

import numpy as np
from pandas import Series, DataFrame

print '用字典生成DataFrame，key为列的名字。'
data = {'state':['Ohio', 'Ohio', 'Ohio', 'Nevada', 'Nevada'],
        'year':[2000, 2001, 2002, 2001, 2002],
        'pop':[1.5, 1.7, 3.6, 2.4, 2.9]}
print DataFrame(data)
print DataFrame(data, columns = ['year', 'state', 'pop']) # 指定列顺序
print '------------------'

print '指定索引，在列中指定不存在的列，默认数据用NaN。'
frame2 = DataFrame(data,
                    columns = ['year', 'state', 'pop', 'debt'],
                    index = ['one', 'two', 'three', 'four', 'five'])
print frame2
print frame2['state']
print frame2.year
print frame2.ix['three']
frame2['debt'] = 16.5 # 修改一整列
print frame2
frame2.debt = np.arange(5)  # 用numpy数组修改元素
print frame2
print '------------------'

print '用Series指定要修改的索引及其对应的值，没有指定的默认数据用NaN。'
val = Series([-1.2, -1.5, -1.7], index = ['two', 'four', 'five'])
frame2['debt'] = val
print frame2
print '------------------'

print '赋值给新列'
frame2['eastern'] = (frame2.state == 'Ohio')  # 如果state等于Ohio为True
print frame2
print frame2.columns
print '------------------'

print 'DataFrame转置'
pop = {'Nevada':{2001:2.4, 2002:2.9},
        'Ohio':{2000:1.5, 2001:1.7, 2002:3.6}}
frame3 = DataFrame(pop)
print frame3
print frame3.T
print '------------------'

print '指定索引顺序，以及使用切片初始化数据。'
print DataFrame(pop, index = [2001, 2002, 2003])
pdata = {'Ohio':frame3['Ohio'][:-1], 'Nevada':frame3['Nevada'][:2]}
print DataFrame(pdata)
print '------------------'

print '指定索引和列的名称'
frame3.index.name = 'year'
frame3.columns.name = 'state'
print frame3
print frame3.values
print frame2.values

'''
数据结果 索引对象
    1.pandas的索引对象负责管理轴标签和其他元数据(比如轴名称等)。构建Series或者DataFrame时，所用到的任何数组或其他序列的标签都会被换成一个Index
    2.Index对象是不可修改的(immutable)，因此用户不能对其进行修改。不可修改性非常重要，因为这样才能使Index对象在多个数据结构之间安全的共享
    例子：
        introduction_to_pandas_data_structures/index_objects.py

'''
import numpy as np
import pandas as pd
import sys
from pandas import Series, DataFrame, Index

print '获取index'
obj = Series(range(3), index = ['a', 'b', 'c'])
index = obj.index
print index[1:]
try:
    index[1] = 'd'  # index对象read only
except:
    print sys.exc_info()[0]
print

print '使用Index对象'
index = Index(np.arange(3))
obj2 = Series([1.5, -2.5, 0], index = index)
print obj2
print obj2.index is index
print

print '判断列和索引是否存在'
pop = {'Nevada':{20001:2.4, 2002:2.9},
        'Ohio':{2000:1.5, 2001:1.7, 2002:3.6}}
frame3 = DataFrame(pop)
print 'Ohio' in frame3.columns
print '2003' in frame3.index

'''
        
    pandas中的主要的Index对象：
        index : 最泛化的Index对象，将轴标签为一个由Python对象组成的Numpy数组。
        int64Index : 针对整数的特殊Index
        MultiIndex : '层次化'索引对象，表示单个轴上的多层索引。可以看做由原数组组成的数组。
        DatetimeIndex : 存储纳米级的时间戳。
        PeriodIndex : 针对Period数据的特殊Index。
        
    Index的方法和属性：
        append : append连接另一个Index对象，产生一个新的Index。
        diff : 计算差集集，并得到一个Index
        intersection : 计算交集。
        union : 计算并集。
        isin : 计算一个指标各值是否包含在参数集合中的布尔型数组。
        delete : 删除索引i处的元素，并得到新的Index。
        drop : 删除传入的值，并得到新的索引。
        insert : 将元素插入到索引i处，并得到一个新的Index。
        is_monotonic : 各元素均大于等于前一个元素时，返回TRUE。
        is_unique : 当索引没有重复值的时候，返回True.
        unique : 计算Index中的唯一值的数组。
        
基本功能 重新索引：
    1.创建一个适应新索引的新对象，该Series的reindex将会根据新的索引进行重排。如果某个索引值当前不存在，就引入缺失值。
    2.对于时间序列这样的有序数据，重新索引时可能需要做一些插值处理。method选项即可达到此目的。
    例子：
        essential_functionality/reindexing.py
'''

import numpy as np
import pandas as pd
import sys
from pandas import Series, DataFrame, Index

print '获取index'
obj = Series(range(3), index = ['a', 'b', 'c'])
index = obj.index
print index[1:]
try:
    index[1] = 'd'  # index对象read only
except:
    print sys.exc_info()[0]
print

print '使用Index对象'
index = Index(np.arange(3))
obj2 = Series([1.5, -2.5, 0], index = index)
print obj2
print obj2.index is index
print

print '判断列和索引是否存在'
pop = {'Nevada':{20001:2.4, 2002:2.9},
        'Ohio':{2000:1.5, 2001:1.7, 2002:3.6}}
frame3 = DataFrame(pop)
print 'Ohio' in frame3.columns
print '2003' in frame3.index



'''
    reindex函数的参数：
        index : 用作索引的新序列。即可以是Index实例，也可以是其它序列型的Python数据结构。Index会被完全使用，就像没有任何复制一样。
        method : 插值填充方式，ffill或bfill。
        fill_value : 在重新索引的过程中，需要引入缺失值时使用的替换值。
        limit : 前向或后向填充时的最大充量。
        level : 在MultiIndex的指定级别上匹配简单的索引，否则选取其子集。
        copy : 默认为True,无论如何复制。如果为FALSE，则新旧相等就不复制。

基本功能 丢弃指定轴上项：
    丢弃某个轴上的一个或者多个项很简单，只要有一个索引数组或者列表即可。由于需要执行一些数据整理和集合逻辑，所以drop方法返回的是一个指定轴上删除了指定值的新对象。
    例子：
        essential_functionality/dropping_entries_from_an_axis.py
'''

import numpy as np
from pandas import Series, DataFrame

print 'Series根据索引删除元素'
obj = Series(np.arange(5.), index = ['a', 'b', 'c', 'd', 'e'])
new_obj = obj.drop('c')
print new_obj
print obj.drop(['d', 'c'])
print

print 'DataFrame删除元素，可指定索引或列。'
data = DataFrame(np.arange(16).reshape((4, 4)),
                  index = ['Ohio', 'Colorado', 'Utah', 'New York'],
                  columns = ['one', 'two', 'three', 'four'])
print data
print data.drop(['Colorado', 'Ohio'])
print data.drop('two', axis = 1)
print data.drop(['two', 'four'], axis = 1)



'''
基本功能 索引、选取和过滤：
    1.Series索引（obj[...]）的工作方式似乎于NumPy数组的索引，只不过Series的索引值不只是整数。
    2.利用标签的切片运算与普通的Python切片运算不同，其末端是包含的（inclusive）
    3.对DataFrame进行索引其实就是获取一个或多个列
    4.为了在DataFrame的行上进行标签索引，引入了专门的索引字段ix.
    例子：
        essential_functionality/indexing_selection_and_filtering.py

'''

import numpy as np
from pandas import Series, DataFrame

print 'Series的索引，默认数字索引可以工作。'
obj = Series(np.arange(4.), index = ['a', 'b', 'c', 'd'])
print obj['b']
print obj[3]
print obj[[1, 3]]
print obj[obj < 2]
print

print 'Series的数组切片'
print obj['b':'c']  # 闭区间
obj['b':'c'] = 5
print obj
print

print 'DataFrame的索引'
data = DataFrame(np.arange(16).reshape((4, 4)),
                  index = ['Ohio', 'Colorado', 'Utah', 'New York'],
                  columns = ['one', 'two', 'three', 'four'])
print data
print data['two'] # 打印列
print data[['three', 'one']]
print data[:2]
print data.ix['Colorado', ['two', 'three']] # 指定索引和列
print data.ix[['Colorado', 'Utah'], [3, 0, 1]]
print data.ix[2]  # 打印第2行（从0开始）
print data.ix[:'Utah', 'two'] # 从开始到Utah，第2列。
print

print '根据条件选择'
print data[data.three > 5]
print data < 5  # 打印True或者False
data[data < 5] = 0
print data


'''
    DataFrame的索引选项：
        obj[val] : 选取DataFrame的单个列或一组列。在一些特殊的情况下会比较便利：布尔型数组（过滤行）、切片（行切片）、布尔型DataFrame（根据条件设置值）
        obj.ix[val] : 选取DataFrame的单个行或者一组行。
        obj.ix[val1,val] : 同时选取行和列。
        reindex方法 ： 将一个轴或多个轴匹配到新的索引
        xs方法 ： 根据整数位置选取单行或者单列，并返回一个Series.
        icol、irow方法 : 根据整数位置选取单行或者单列，并返回一个Series。
        get_value,set_value方法 ： 根据行标签或者列标签选取单个值。
        
基本功能 算术运算和数据对齐
    1.对不同的索引对象进行算术运算。
    2.自动数据对齐在不重叠的索引处引入NA值，缺失值会在算术运算过程中传播。
    3.对于DataFrame，对齐操作会发生在行和列上。
    4.fill_value参数
    5.DataFrame和Series之间的运算
    例子：
        essential_functionality/arithmetic_and_data_alignment.py

'''

import numpy as np
from pandas import Series, DataFrame

print '加法'
s1 = Series([7.3, -2.5, 3.4, 1.5], index = ['a', 'c', 'd', 'e'])
s2 = Series([-2.1, 3.6, -1.5, 4, 3.1], index = ['a', 'c', 'e', 'f', 'g'])
print s1
print s2
print s1 + s2
print

print 'DataFrame加法，索引和列都必须匹配。'
df1 = DataFrame(np.arange(9.).reshape((3, 3)),
                columns = list('bcd'),
                index = ['Ohio', 'Texas', 'Colorado'])
df2 = DataFrame(np.arange(12).reshape((4, 3)),
                columns = list('bde'),
                index = ['Utah', 'Ohio', 'Texas', 'Oregon'])
print df1
print df2
print df1 + df2
print

print '数据填充'
df1 = DataFrame(np.arange(12.).reshape((3, 4)), columns = list('abcd'))
df2 = DataFrame(np.arange(20.).reshape((4, 5)), columns = list('abcde'))
print df1
print df2
print df1.add(df2, fill_value = 0)
print df1.reindex(columns = df2.columns, fill_value = 0)
print

print 'DataFrame与Series之间的操作'
arr = np.arange(12.).reshape((3, 4))
print arr
print arr[0]
print arr - arr[0]
frame = DataFrame(np.arange(12).reshape((4, 3)),
                  columns = list('bde'),
                  index = ['Utah', 'Ohio', 'Texas', 'Oregon'])
series = frame.ix[0]
print frame
print series
print frame - series
series2 = Series(range(3), index = list('bef'))
print frame + series2
series3 = frame['d']
print frame.sub(series3, axis = 0)  # 按列减


'''
        
基本功能 函数应用和映射：
    1.numpy的ufuncs(元素级数组方法)
    2.DataFrame的apply方法
    3.对象的applymap方法（因为Series有一个应用于元素级的map方法）
    例子：
        essential_functionality/function_application_and_mapping.py
'''

import numpy as np
from pandas import Series, DataFrame

print '函数'
frame = DataFrame(np.random.randn(4, 3),
                  columns = list('bde'),
                  index = ['Utah', 'Ohio', 'Texas', 'Oregon'])
print frame
print np.abs(frame)
print

print 'lambda以及应用'
f = lambda x: x.max() - x.min()
print frame.apply(f)
print frame.apply(f, axis = 1)
def f(x):
    return Series([x.min(), x.max()], index = ['min', 'max'])
print frame.apply(f)
print

print 'applymap和map'
_format = lambda x: '%.2f' % x
print frame.applymap(_format)
print frame['e'].map(_format)


'''
基本功能 排序和排名：
    1.对行和列索引进行排序。
    2.对于DataFrame,根据任意一个轴上的索引进行排序。
    3.可以指定升序降序。
    4.按值排序
    5.对于DataFrame,可以指定按值排序的列。
    6.rank函数
    例子：
        essential_functionality/sorting_and_ranking.py

'''

import numpy as np
from pandas import Series, DataFrame

print '根据索引排序，对于DataFrame可以指定轴。'
obj = Series(range(4), index = ['d', 'a', 'b', 'c'])
print obj.sort_index()
frame = DataFrame(np.arange(8).reshape((2, 4)),
                  index = ['three', 'one'],
                  columns = list('dabc'))
print frame.sort_index()
print frame.sort_index(axis = 1)
print frame.sort_index(axis = 1, ascending = False) # 降序
print

print '根据值排序'
obj = Series([4, 7, -3, 2])
print obj.sort_values() # order已淘汰
print

print 'DataFrame指定列排序'
frame = DataFrame({'b':[4, 7, -3, 2], 'a':[0, 1, 0, 1]})
print frame
print frame.sort_values(by = 'b') # sort_index(by = ...)已淘汰
print frame.sort_values(by = ['a', 'b'])
print

print 'rank，求排名的平均位置(从1开始)'
obj = Series([7, -5, 7, 4, 2, 0, 4])
# 对应排名：-5(1), 0(2), 2(3), 4(4), 4(5), 7(6), 7(7)
print obj.rank()
print obj.rank(method = 'first')  # 去第一次出现，不求平均值。
print obj.rank(ascending = False, method = 'max') # 逆序，并取最大值。所以-5的rank是7.
frame = DataFrame({'b':[4.3, 7, -3, 2],
                  'a':[0, 1, 0, 1],
                  'c':[-2, 5, 8, -2.5]})
print frame
print frame.rank(axis = 1)

'''
基本功能 带有重复值的索引：
    1.对于重复索引，返回Series,对应单个值的索引返回标量。
    例子：
        essential_functionality/axis_indexes_with_duplicate_values.py

'''

import numpy as np
from pandas import Series, DataFrame

print '重复的索引'
obj = Series(range(5), index = ['a', 'a', 'b', 'b', 'c'])
print obj.index.is_unique # 判断是非有重复索引
print obj['a'][0], obj.a[1]
df = DataFrame(np.random.randn(4, 3), index = ['a', 'a', 'b', 'b'])
print df
print df.ix['b'].ix[0]
print df.ix['b'].ix[1]


'''
汇总和计算描述统计：
    常用方法选项：
        axis : 指定轴，DataFrame的行用0，列用1.
        skipna : 排除缺失值，默认值为True。
        level ： 如果轴是层次化索引的（MultiIndex）,则根据level选取分组。
    常用描述和汇总统计函数：
        count : 非NA值的数量。
        describe : 针对Series或者DataFrame列计算汇总统计。
        min、max : 计算最小值和最大值。
        argmin、argmax : 计算能够获取到最大值和最小值的索引位置（整数）
        idxmin,idxmax : 计算能够获取到的最小值个最大值的索引值
        sum : 值的总和。
        mean : 值的平均数。
        median : 值的算术中位数。
        mad : 根据平均值计算平均绝对离差。
        var : 样本值的方差。
        std : 样本值的标准差。
        skew : 样本值的偏度（三阶矩）。
        kurt : 样本值的偏度（四阶矩）。
        cumsum : 样本值的累计求和。
        cummin,cummax : 样本值的累计最大值和最小值。
        cumprod : 样本值的累计积。
        diff ： 计算一阶差分。
        pct_change : 计算百分数变化。
        
    1.数值型和非数值型的区别。
    2.NA值被自动排查，除非通过skipna选项。
    例子：
        summarizing_and_computing_descriptive_statistics/intro.py

'''

import numpy as np
from pandas import Series, DataFrame

print '求和'
df = DataFrame([[1.4, np.nan], [7.1, -4.5], [np.nan, np.nan], [0.75, -1.3]],
              index = ['a', 'b', 'c', 'd'],
              columns = ['one', 'two'])
print df
print df.sum()  # 按列求和
print df.sum(axis = 1)  # 按行求和
print

print '平均数'
print df.mean(axis = 1, skipna = False)
print df.mean(axis = 1)
print

print '其它'
print df.idxmax()
print df.cumsum()
print df.describe()
obj = Series(['a', 'a', 'b', 'c'] * 4)
print obj.describe()


'''
汇总和计算描述统计 相关系数和协方差：
    1.相关系数：相关系数是用以反应变量之间的相关关系密切程度的统计指标。
    2.协方差 ： 从直观上来看，协方差表示的是两个变量总体误差期望。如果两个变量的变化趋势一致，也就是说如果其中一个大于自身的期望值时，另外一个也大于
               自身的期望值，那么两个变量之间的协方差就是正值；乳沟两个变量变化趋势相反，即其中一个变量大于自身的期望值时另外一个小于自身的期望值，那么两个变量之间的协方差就是负值。
    例子：
        summarizing_and_computing_descriptive_statistics/correlation_and_covariance.py
'''

import numpy as np
import pandas.io.data as web
from pandas import DataFrame

print '相关性与协方差'  # 协方差：https://zh.wikipedia.org/wiki/%E5%8D%8F%E6%96%B9%E5%B7%AE
all_data = {}
for ticker in ['AAPL', 'IBM', 'MSFT', 'GOOG']:
    all_data[ticker] = web.get_data_yahoo(ticker, '4/1/2016', '7/15/2015')
    price = DataFrame({tic: data['Adj Close'] for tic, data in all_data.iteritems()})
    volume = DataFrame({tic: data['Volume'] for tic, data in all_data.iteritems()})
returns = price.pct_change()
print returns.tail()
print returns.MSFT.corr(returns.IBM)
print returns.corr()  # 相关性，自己和自己的相关性总是1
print returns.cov() # 协方差
print returns.corrwith(returns.IBM)
print returns.corrwith(returns.volume)

'''
        
汇总和计算描述统计 唯一值以及成员资格：
    常用方法：
        is_in : 计算一个表示'Series各值是否包含于传入的值的序列中'的布尔数组。
        unique : 计算Series中唯一值的数组，按发现的顺序返回。
        value_counts : 返回一个Series，其索引为唯一值，其值为频率，按计数值降序排列。
    例子：
        correlation_and_covariance/unique_values_value_counts_and_membership.py

'''

import numpy as np
import pandas as pd
from pandas import Series, DataFrame

print '去重'
obj = Series(['c', 'a', 'd', 'a', 'a', 'b', 'b', 'c', 'c'])
print obj.unique()
print obj.value_counts()
print

print '判断元素存在'
mask = obj.isin(['b', 'c'])
print mask
print obj[mask] #只打印元素b和c
data = DataFrame({'Qu1':[1, 3, 4, 3, 4],
                  'Qu2':[2, 3, 1, 2, 3],
                  'Qu3':[1, 5, 2, 4, 4]})
print data
print data.apply(pd.value_counts).fillna(0)
print data.apply(pd.value_counts, axis = 1).fillna(0)

'''
处理缺失数据：
    NA处理方法：
        dropna : 根据各标签的值中是否存在缺少数据对轴。
        fillba : 样本值的标准差。
        isnull : 样本值的偏度（三阶矩）。
    NaN（Not a Number）表示浮点数和非浮点数组中的缺失数据
    None也被当NA处理
    例子：
        handling_missing_data/intro.py

'''

import numpy as np
from pandas import Series

print '作为null处理的值'
string_data = Series(['aardvark', 'artichoke', np.nan, 'avocado'])
print string_data
print string_data.isnull()
string_data[0] = None
print string_data.isnull()

'''
处理缺失数据 滤除缺失数据：
    dropna
    布尔索引
    DatFrame默认丢弃任何含有缺失值的行
    how参数控制行为，axis参数选择轴，thresh参数控制留下来的数量
    例子：
        handing_missing_data/filtering_out_missing_data.py

'''
import numpy as np
from numpy import nan as NA
from pandas import Series, DataFrame

print '丢弃NA'
data = Series([1, NA, 3.5, NA, 7])
print data.dropna()
print data[data.notnull()]
print

print 'DataFrame对丢弃NA的处理'
data = DataFrame([[1., 6.5, 3.], [1., NA, NA],
                  [NA, NA, NA], [NA, 6.5, 3.]])
print data.dropna() # 默认只要某行有NA就全部删除
print data.dropna(how = 'all')  # 全部为NA才删除
data[4] = NA  # 新增一列
print data.dropna(axis = 1, how = 'all')
data = DataFrame(np.random.randn(7, 3))
data.ix[:4, 1] = NA
data.ix[:2, 2] = NA
print data
print data.dropna(thresh = 2) # 每行至少要有2个非NA元素


'''
处理缺失数据 填充缺失数据
    fillna
    inplace参数控制返回新对象还是就地修改
    例子：
        handing_missing_data/filling_in_missing_data.py

'''
import numpy as np
from numpy import nan as NA
import pandas as pd
from pandas import Series, DataFrame, Index

print '填充0'
df = DataFrame(np.random.randn(7, 3))
df.ix[:4, 1] = NA
df.ix[:2, 2] = NA
print df.fillna(0)
df.fillna(0, inplace = True)
print df
print

print '不同行列填充不同的值'
print df.fillna({1:0.5, 3:-1})  # 第3列不存在
print

print '不同的填充方式'
df = DataFrame(np.random.randn(6, 3))
df.ix[2:, 1] = NA
df.ix[4:, 2] = NA
print df
print df.fillna(method = 'ffill')
print df.fillna(method = 'ffill', limit = 2)
print

print '用统计数据填充'
data = Series([1., NA, 3.5, NA, 7])
print data.fillna(data.mean())



'''

层次化索引：
    1.使你能在一个轴上拥有多个（两个以上）索引级别。抽象的说，它使你能以低纬度形式处理高维度数据。
    2.通过stack与unstack变换DataFrame。
    例子：
        hierarchical_indexing/intro.py
'''
import numpy as np
from pandas import Series, DataFrame, MultiIndex

print 'Series的层次索引'
data = Series(np.random.randn(10),
              index = [['a', 'a', 'a', 'b', 'b', 'b', 'c', 'c', 'd', 'd'],
                       [1, 2, 3, 1, 2, 3, 1, 2, 2, 3]])
print data
print data.index
print data.b
print data['b':'c']
print data[:2]
print data.unstack()
print data.unstack().stack()
print

print 'DataFrame的层次索引'
frame = DataFrame(np.arange(12).reshape((4, 3)),
                  index = [['a', 'a', 'b', 'b'], [1, 2, 1, 2]],
                  columns = [['Ohio', 'Ohio', 'Colorado'], ['Green', 'Red', 'Green']])
print frame
frame.index.names = ['key1', 'key2']
frame.columns.names = ['state', 'color']
print frame
print frame.ix['a', 1]
print frame.ix['a', 2]['Colorado']
print frame.ix['a', 2]['Ohio']['Red']
print

print '直接用MultiIndex创建层次索引结构'
print MultiIndex.from_arrays([['Ohio', 'Ohio', 'Colorado'], ['Gree', 'Red', 'Green']],
                             names = ['state', 'color'])




'''
         
层次化索引 重新分级顺序：
    索引交换
    索引重新排序
    例子：
        hierarchical_indexing/reordering_and_sorting_levels.py

'''
import numpy as np
from pandas import Series, DataFrame

print '索引层级交换'
frame = DataFrame(np.arange(12).reshape((4, 3)),
                  index = [['a', 'a', 'b', 'b'], [1, 2, 1, 2]],
                  columns = [['Ohio', 'Ohio', 'Colorado'], ['Green', 'Red', 'Green']])
frame.index.names = ['key1', 'key2']
frame_swapped = frame.swaplevel('key1', 'key2')
print frame_swapped
print frame_swapped.swaplevel(0, 1)
print

print '根据索引排序'
print frame.sortlevel('key2')
print frame.swaplevel(0, 1).sortlevel(0)


'''
        
层次化索引 根据级别汇总统计：
    指定索引级别和轴
    例子：
        hierarchical_indexing/summmary_statistics_by_level.py

'''
import numpy as np
from pandas import DataFrame

print '根据指定的key计算统计信息'
frame = DataFrame(np.arange(12).reshape((4, 3)),
                  index = [['a', 'a', 'b', 'b'], [1, 2, 1, 2]],
                  columns = [['Ohio', 'Ohio', 'Colorado'], ['Green', 'Red', 'Green']])
frame.index.names = ['key1', 'key2']
print frame
print frame.sum(level = 'key2')



'''
        
层次化索引 使用DataFrame的列：
    1.将指定列变为索引
    2.移除或保留对象
    3.reset_index恢复
    例子：
        hierarchical_indexing/using_a_dataframes_columns.py

'''

import numpy as np
from pandas import DataFrame

print '使用列生成层次索引'
frame = DataFrame({'a':range(7),
                   'b':range(7, 0, -1),
                   'c':['one', 'one', 'one', 'two', 'two', 'two', 'two'],
                   'd':[0, 1, 2, 0, 1, 2, 3]})
print frame
print frame.set_index(['c', 'd'])  # 把c/d列变成索引
print frame.set_index(['c', 'd'], drop = False) # 列依然保留
frame2 = frame.set_index(['c', 'd'])
print frame2.reset_index()


'''   
      
其他话题 整数索引：
    1.歧义的产生
    2.可靠的，不考虑所引类型的，基于位置的索引
    例子：
        other_pandas_topic/integer_indexing.py
        
'''

import numpy as np
import sys
from pandas import Series, DataFrame

print '整数索引'
ser = Series(np.arange(3.))
print ser
try:
    print ser[-1] # 这里会有歧义
except:
    print sys.exc_info()[0]
ser2 = Series(np.arange(3.), index = ['a', 'b', 'c'])
print ser2[-1]
ser3 = Series(range(3), index = [-5, 1, 3])
print ser3.iloc[2]  # 避免直接用[2]产生的歧义
print

print '对DataFrame使用整数索引'
frame = DataFrame(np.arange(6).reshape((3, 2)), index = [2, 0, 1])
print frame
print frame.iloc[0]
print frame.iloc[:, 1]



import numpy as np
import pandas as pd
import pandas.io.data as web
from pandas import Series, DataFrame, Index, Panel

pdata = Panel(dict((stk, web.get_data_yahoo(stk, '1/1/2016', '1/15/2016')) for stk in ['AAPL', 'GOOG', 'BIDU', 'MSFT']))
print pdata
pdata = pdata.swapaxes('items', 'minor')
print pdata
print

print "访问顺序：# Item -> Major -> Minor"
print pdata['Adj Close']
print pdata[:, '1/5/2016', :]
print pdata['Adj Close', '1/6/2016', :]
print

print 'Panel与DataFrame相互转换'
stacked = pdata.ix[:, '1/7/2016':, :].to_frame()
print stacked
print stacked.to_panel()

'''
extral 
'''

import numpy as np
import pandas as pd

names = ['date',
         'time',
         'opening_price',
         'ceiling_price',
         'floor_price',
         'closing_price',
         'volume',
         'amount']
raw = pd.read_csv('SH600690.csv', names = names, header = None, index_col='date', parse_dates=True)
print raw.head()
print

'''
# 根据涨跌幅判断数据是否有效
def _valid_price(prices):
    return (((prices.max() - prices.min()) / prices.min()) < 0.223).all()

# 按日期分组
days = raw.groupby(level = 0).agg(
        {'opening_price':lambda prices: _valid_price(prices) and prices[0] or 0,
         'ceiling_price':lambda prices: _valid_price(prices) and np.max(prices) or 0,
         'floor_price':lambda prices: _valid_price(prices) and np.min(prices) or 0,
         'closing_price':lambda prices: _valid_price(prices) and prices[-1] or 0,
         'volume':'sum',
         'amount':'sum'})
print days.head()
print


# 缺少数据处理，因为周末没有交易。
start = days.iloc[0:1].index.tolist()[0]
end = days.iloc[-2:-1].index.tolist()[0]
new_idx = pd.date_range(start = start, end = end)
print new_idx
data = days.reindex(new_idx)    # 重新索引
zero_values = data.loc[~(data.volume > 0)].loc[:, ['volume', 'amount']]
data.update(zero_values.fillna(0))  # 交易量和金额填0
data.fillna(method = 'ffill', inplace = True)   # 价格用前一天的填充
print data.head()
print

# 计算30各自然日里的股票平均波动周率
def gen_item_group_index(total, group_len):
    group_count = total / group_len
    group_index = np.arange(total)
    for i in xrange(group_count):
        group_index[i * group_len: (i+ 1) * group_len] = i
    group_index[(i + 1) * group_len:] = i +1
    return group_index.tolist()

period = 30
group_index = gen_item_group_index(len(data), period)
data['group_index'] = group_index
print data.head().append(data.tail())

# 为负表示先出现最高价再出现最低价，即下跌波动。
def _ceiling_price(prices):
    return prices.idxmin() < prices.idxmax() and np.max(prices) or (-np.max(prices))

group = data.groupby('group_index').agg(
            {'volume': 'sum',
             'floor_price': 'min',
             'ceiling_price': _ceiling_price})
print group.head()
date_col = pd.DataFrame({'group_index': group_index, 'date': new_idx})
print date_col
group['date'] = date_col.groupby('group_index').agg('first')    # 为每个索引添加开始日期
print group.head()
group['ripples_ratio'] = group.ceiling_price / group.floor_price    # 计算并添加波动率
print group.head()
print

# 波动率排序
ripples = group.sort_values('ripples_ratio', ascending = False)
print ripples
print ripples.head(10).ripples_ratio.mean()
print ripples.tail(10).ripples_ratio.mean()
print

# 计算涨跌幅
rise = data.closing_price.diff()
data['rise'] = rise
print data.head()
'''