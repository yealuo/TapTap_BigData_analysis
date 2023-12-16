import ast
import csv

from pyspark.sql import Row
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import desc

spark = SparkSession.builder \
    .appName('test') \
    .master("local[*]") \
    .config("spark.sql.shuffle.partition", "2") \
    .config("spark.sql.warehouse.dir", "hdfs://hadoop007:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hadoop007:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 进入taptap_analysis数据库
spark.sql('use taptap_analysis')

# 读取数据到DataFrame
hot_android_df = spark.read.csv('/data/taptap_data/hot_android.csv', inferSchema=True, header=True)
hotplay_android_df = spark.read.csv('/data/taptap_data/hotplay_android.csv', inferSchema=True, header=True)
book_android_df = spark.read.csv('/data/taptap_data/book_android.csv', inferSchema=True, header=True)
hot_ios_df = spark.read.csv('/data/taptap_data/hot_ios.csv', inferSchema=True, header=True)
book_ios_df = spark.read.csv('/data/taptap_data/book_ios.csv', inferSchema=True, header=True)

# 删除空白数据、重命名列
hot_android_df = hot_android_df.filter(
    (hot_android_df.游戏名 != 'null') & (hot_android_df.标签 != 'null')) \
    .withColumnRenamed('游戏名', 'game') \
    .withColumnRenamed('标签', 'tag') \
    .withColumnRenamed('评分', 'score')
hotplay_android_df = hotplay_android_df.filter(
    (hotplay_android_df.游戏名 != 'null') & (hotplay_android_df.标签 != 'null')) \
    .withColumnRenamed('游戏名', 'game') \
    .withColumnRenamed('标签', 'tag') \
    .withColumnRenamed('评分', 'score')
book_android_df = book_android_df.filter(
    (book_android_df.游戏名 != 'null') & (book_android_df.标签 != 'null')) \
    .withColumnRenamed('游戏名', 'game') \
    .withColumnRenamed('标签', 'tag') \
    .withColumnRenamed('评分', 'score')
hot_ios_df = hot_ios_df.filter(
    (hot_ios_df.游戏名 != 'null') & (hot_ios_df.标签 != 'null')) \
    .withColumnRenamed('游戏名', 'game') \
    .withColumnRenamed('标签', 'tag') \
    .withColumnRenamed('评分', 'score')
book_ios_df = book_ios_df.filter(
    (book_ios_df.游戏名 != 'null') & (book_ios_df.标签 != 'null')) \
    .withColumnRenamed('游戏名', 'game') \
    .withColumnRenamed('标签', 'tag') \
    .withColumnRenamed('评分', 'score')

# 向表中写入数据
hot_android_df.write.mode('overwrite').format('hive').saveAsTable('hot_android')
hotplay_android_df.write.mode('overwrite').format('hive').saveAsTable('hotplay_android')
book_android_df.write.mode('overwrite').format('hive').saveAsTable('book_android')
hot_ios_df.write.mode('overwrite').format('hive').saveAsTable('hot_ios')
book_ios_df.write.mode('overwrite').format('hive').saveAsTable('book_ios')

# 从数据库读入数据
hot_android_df = spark.sql('select * from hot_android')
hotplay_android_df = spark.sql('select * from hotplay_android')
book_android_df = spark.sql('select * from book_android')
hot_ios_df = spark.sql('select * from hot_ios')
book_ios_df = spark.sql('select * from book_ios')

'''
研究内容：
热门榜（每日下载量）（排名权值：0.6     评分权值：0.4）：
    前10名展示（表格）
    各tag占比(相同tag数/游戏数）（柱状图）
    榜中各游戏拥有的tag数量占比（饼状图）
    榜中所有游戏按类型分类，取类型所含游戏数前10多里面的前5名游戏展示（表格）
    高分游戏（前30名）各tag占比数(相同tag数/游戏数）（柱状图）
    综合榜单排名和评分重新排名取前10（算法）（表格）
热玩榜（日活）（排名权值：0.5   评分权值：0.5）：
    前10名展示（表格）
    各tag占比(相同tag数/游戏数）（柱状图）
    榜中各游戏拥有的tag数量占比（饼状图）
    榜中所有游戏按类型分类，取类型所含游戏数前10多里面的前5名游戏展示（表格）
    高分游戏（前30名）各tag占比数(相同tag数/游戏数）（柱状图）
    综合榜单排名和评分重新排名取前10（算法）（表格）
预约榜（预约量）（排名权值：0.3   评分权值：0.7）：
    前10名展示（表格）
    各tag占比(相同tag数/游戏数）（柱状图）
    榜中各游戏拥有的tag数量占比（饼状图）
    榜中所有游戏按类型分类，取类型所含游戏数前10多里面的前5名游戏展示（表格）
    高分游戏（前30名）各tag占比数(相同tag数/游戏数）（柱状图）
    综合榜单排名和评分重新排名取前10（算法）（表格）
    
算法：1.排名、分数归一化
     2.归一化排名∗权值-归一化分数*权值=综合分数
     3.综合分数按从小到大排序
'''


# 结果输出1
def output_result1(df: DataFrame, name: str) -> None:
    pandas_df = df.limit(10).toPandas()
    pandas_df.to_csv(f'./前10名展示（表格）/{name}.csv', index=False)


# 各tag占比
def tag_count(df: DataFrame) -> dict:
    tag_list = df.select('tag').rdd.flatMap(lambda x: x).collect()  # 将column类型转为list类型
    for i in range(len(tag_list)):
        tag_list[i] = ast.literal_eval(tag_list[i])  # 将列表中的string元素转换为list类型
    tag_dict = {}
    for i in tag_list:
        for j in i:
            tag_dict[j] = 0  # 初始化计数字典
    for i in tag_list:
        for j in i:
            tag_dict[j] += 1  # 统计tag数量
    tag_dict = dict(sorted(tag_dict.items(), key=lambda x: x[1], reverse=True))  # 按出现次数进行排序
    for key, value in tag_dict.items():
        value = float(value) * 100 / df.count()
        tag_dict[key] = f'{value:.2f}%'
    return tag_dict


# 结果输出2
def output_result2(result_dict: dict, name: str) -> None:
    with open(f'./各tag占比/{name}.csv', 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(('tag', 'account'))
    with open(f'./各tag占比/{name}.csv', 'a', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        count = 0  # 计数器
        for data in result_dict.items():
            if count >= 10:
                break
            writer.writerow(data)
            count += 1


# 各游戏拥有tag数量统计
def game_tag_count(df: DataFrame) -> dict:
    tag_list = df.select('tag').rdd.flatMap(lambda x: x).collect()  # 将column类型转为list类型
    for i in range(len(tag_list)):
        tag_list[i] = ast.literal_eval(tag_list[i])  # 将列表中的string元素转换为list类型
    tag_num = {1: 0, 2: 0, 3: 0}
    for i in tag_list:
        tag_num[len(i)] += 1
    for key, value in tag_num.items():
        value = float(value) * 100 / df.count()
        tag_num[key] = f'{value:.2f}%'
    return tag_num


# 结果输出3
def output_result3(result_dict: dict, name: str) -> None:
    with open(f'./榜中各游戏拥有的tag数量占比（饼状图）/{name}.csv', 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(('number_of_tags_a_game_has', 'account'))
    with open(f'./榜中各游戏拥有的tag数量占比（饼状图）/{name}.csv', 'a', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        for data in result_dict.items():
            writer.writerow(data)


# 取类型所含游戏数前10多里面的前5名
def tag_group(tag_dict: dict, df: DataFrame) -> dict:
    game_list = df.collect()  # 将DataFrame转为list
    game_in_tag = {}  # 结果字典{tag:[game,......]}
    for tag in tag_dict:
        if len(game_in_tag) == 10:  # 控制取前10个tag
            break
        tag_game_list = []  # 包含某种tag的游戏列表
        for game_data in game_list:
            if len(tag_game_list) == 5:  # 控制取前5名游戏
                break
            game_data_tag_list = ast.literal_eval(game_data.tag)  # 将列表中的string元素转换为list类型
            if tag in game_data_tag_list:
                tag_game_list.append(game_data.game)
        game_in_tag[tag] = tag_game_list
    return game_in_tag


# 结果输出4
def output_result4(result_dict: dict, name: str) -> None:
    with open(f'./榜中所有游戏按类型分类，取类型所含游戏数前10多里面的前5名游戏展示（表格）/{name}.csv', 'w',
              encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(('tag', 'game'))
    with open(f'./榜中所有游戏按类型分类，取类型所含游戏数前10多里面的前5名游戏展示（表格）/{name}.csv', 'a',
              encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        for data in result_dict.items():
            writer.writerow(data)


# 结果输出5
def output_result5(result_dict: dict, name: str) -> None:
    with open(f'./高分游戏（前30名）各tag占比数(相同tag数游戏数）（柱状图）/{name}.csv', 'w', encoding='utf-8',
              newline='') as f:
        writer = csv.writer(f)
        writer.writerow(('tag', 'account'))
    with open(f'./高分游戏（前30名）各tag占比数(相同tag数游戏数）（柱状图）/{name}.csv', 'a', encoding='utf-8',
              newline='') as f:
        writer = csv.writer(f)
        count = 0  # 计数器
        for data in result_dict.items():
            if count >= 10:
                break
            writer.writerow(data)
            count += 1


# 综合排名算法
def comprehensive_ranking(df: DataFrame, w1: float, w2: float) -> DataFrame:
    row_count = df.count()  # 获取总行数
    min_score = 0.0
    max_score = 0.0
    for row in df.rdd.collect():
        min_score = min(min_score, float(row.score))  # 获取最低分
        max_score = max(max_score, float(row.score))  # 获取最高分
    df_new = []  # 新DataFrame
    i = 0
    for row in df.rdd.collect():
        i += 1  # 获取当前行号
        normalized_ranking = (i - 1) / (row_count - 1)  # 获取归一化排名
        normalized_score = (float(row.score) - min_score) / (max_score - min_score)  # 获取归一化分数
        comprehensive_score = normalized_ranking * w1 - normalized_score * w2  # 计算加权分
        new_row = Row(game=row.game, tag=row.tag, score=row.score, comprehensive_score=comprehensive_score)  # 构造新Row
        df_new.append(new_row)
    df_new = spark.createDataFrame(df_new, ['game', 'tag', 'score', 'comprehensive_score']).orderBy(
        'comprehensive_score')  # 构造新DataFrame
    df_new = df_new.drop('comprehensive_score')
    return df_new


# 结果输出6
def output_result6(df: DataFrame, name: str) -> None:
    pandas_df = df.limit(10).toPandas()
    pandas_df.to_csv(f'./综合榜单排名和评分重新排名取前10（算法）（表格）/{name}.csv', index=False)


# 前10名展示（表格）
output_result1(hot_android_df, 'hot_android')  # 接口
output_result1(hotplay_android_df, 'hotplay_android')  # 接口
output_result1(book_android_df, 'book_android')  # 接口
output_result1(hot_ios_df, 'hot_ios')  # 接口
output_result1(book_ios_df, 'book_ios')  # 接口

# 各tag数量
hot_android_tag_count = tag_count(hot_android_df)
hotplay_android_tag_count = tag_count(hotplay_android_df)
book_android_tag_count = tag_count(book_android_df)
hot_ios_tag_count = tag_count(hot_ios_df)
book_ios_tag_count = tag_count(book_ios_df)
output_result2(hot_android_tag_count, 'hot_android_tag_count')  # 接口
output_result2(hotplay_android_tag_count, 'hotplay_android_tag_count')  # 接口
output_result2(book_android_tag_count, 'book_android_tag_count')  # 接口
output_result2(hot_ios_tag_count, 'hot_ios_tag_count')  # 接口
output_result2(book_ios_tag_count, 'book_ios_tag_count')  # 接口

# 榜中各游戏tag数量占比
hot_android_tag_num = game_tag_count(hot_android_df)
hotplay_android_tag_num = game_tag_count(hotplay_android_df)
book_android_tag_num = game_tag_count(book_android_df)
hot_ios_tag_num = game_tag_count(hot_ios_df)
book_ios_tag_num = game_tag_count(book_ios_df)
output_result3(hot_android_tag_num, 'hot_android_tag_num')  # 接口
output_result3(hotplay_android_tag_num, 'hotplay_android_tag_num')  # 接口
output_result3(book_android_tag_num, 'book_android_tag_num')  # 接口
output_result3(hot_ios_tag_num, 'hot_ios_tag_num')  # 接口
output_result3(book_ios_tag_num, 'book_ios_tag_num')  # 接口

# 榜中所有游戏按类型分类，取类型所含游戏数前10多里面的前5名游戏展示（表格）
hot_android_game_in_tag = tag_group(hot_android_tag_count, hot_android_df)
hotplay_android_game_in_tag = tag_group(hotplay_android_tag_count, hotplay_android_df)
book_android_game_in_tag = tag_group(book_android_tag_count, book_android_df)
hot_ios_game_in_tag = tag_group(hot_ios_tag_count, hot_ios_df)
book_ios_game_in_tag = tag_group(book_ios_tag_count, book_ios_df)
output_result4(hot_android_game_in_tag, 'hot_android_game_in_tag')  # 接口
output_result4(hotplay_android_game_in_tag, 'hotplay_android_game_in_tag')  # 接口
output_result4(book_android_game_in_tag, 'book_android_game_in_tag')  # 接口
output_result4(hot_ios_game_in_tag, 'hot_ios_game_in_tag')  # 接口
output_result4(book_ios_game_in_tag, 'book_ios_game_in_tag')  # 接口

# 高分游戏（前30名）各tag占比数(相同tag数/游戏数）（柱状图）
hot_android_sorted_df = hot_android_df.orderBy(desc('score')).limit(30)
hotplay_android_sorted_df = hotplay_android_df.orderBy(desc('score')).limit(30)
book_android_sorted_df = book_android_df.orderBy(desc('score')).limit(30)
hot_ios_sorted_df = hot_ios_df.orderBy(desc('score')).limit(30)
book_ios_sorted_df = book_ios_df.orderBy(desc('score')).limit(30)
hot_android_sorted_tag_count = tag_count(hot_android_sorted_df)
hotplay_android_sorted_tag_count = tag_count(hotplay_android_sorted_df)
book_android_sorted_tag_count = tag_count(book_android_sorted_df)
hot_ios_sorted_tag_count = tag_count(hot_ios_sorted_df)
book_ios_sorted_tag_count = tag_count(book_ios_sorted_df)
output_result5(hot_android_sorted_tag_count, 'hot_android_sorted_tag_count')  # 接口
output_result5(hotplay_android_sorted_tag_count, 'hotplay_android_sorted_tag_count')  # 接口
output_result5(book_android_sorted_tag_count, 'book_android_sorted_tag_count')  # 接口
output_result5(hot_ios_sorted_tag_count, 'hot_ios_sorted_tag_count')  # 接口
output_result5(book_ios_sorted_tag_count, 'book_ios_sorted_tag_count')  # 接口

# 综合榜单排名和评分重新排名取前10（算法）（表格）
# 算法：1.排名、分数归一化
#      2.归一化排名∗权值-归一化分数*权值=综合分数
#      3.综合分数按从小到大排序
# hot（排名权值：0.6   评分权值：0.4）
# hotplay（排名权值：0.5   评分权值：0.5）
# book（排名权值：0.3   评分权值：0.7）
hot_android_comprehensive_ranking = comprehensive_ranking(hot_android_df, 0.6, 0.4)
hotplay_android_comprehensive_ranking = comprehensive_ranking(hotplay_android_df, 0.5, 0.5)
book_android_comprehensive_ranking = comprehensive_ranking(book_android_df, 0.3, 0.7)
hot_ios_comprehensive_ranking = comprehensive_ranking(hot_ios_df, 0.6, 0.4)
book_ios_comprehensive_ranking = comprehensive_ranking(book_ios_df, 0.3, 0.7)
output_result6(hot_android_comprehensive_ranking, 'hot_android_comprehensive_ranking')  # 接口
output_result6(hotplay_android_comprehensive_ranking, 'hotplay_android_comprehensive_ranking')  # 接口
output_result6(book_android_comprehensive_ranking, 'book_android_comprehensive_ranking')  # 接口
output_result6(hot_ios_comprehensive_ranking, 'hot_ios_comprehensive_ranking')  # 接口
output_result6(book_ios_comprehensive_ranking, 'book_ios_comprehensive_ranking')  # 接口
