#!/usr/bin/env python

import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import happybase as hb
import pandas as pd

def process_pyspark():

    spark = (
        SparkSession.builder
        .appName("HDFS")
        .getOrCreate()
    )
    
    df_wordcup = (
    spark.read
    .option("header", "true")
    .option("inferschema", "true")
    .parquet('/user/hadoop/project/')
    )
    
    df_lang = (
    df_wordcup.groupby('lang').agg(
    F.count('likeCount').alias('count_lang'),
    F.sum('likeCount').alias('total_likes_lang'),
    F.mean('likeCount').alias('mean_likes_lang'),
    F.max('likeCount').alias('max_likes_lang'),
    ).where(F.col('total_likes_lang') > 20)
    )
    
    df_country = (
    df_wordcup.filter(F.col('lang') == 'en')
    .withColumn('country', 
        F.when(
            F.col('content').like('%Argentina%'), 'Argentina')
            .when(F.col('content').like('%France%'), 'France')
            .when(F.col('content').like('%Croatia%'), 'Croatia')
            .when(F.col('content').like('%Spain%'), 'Spain')
            .when(F.col('content').like('%Poland%'), 'Poland')
            .when(F.col('content').like('%Brasil%'), 'Brasil')
            .when(F.col('content').like('%Germany%'), 'Germany')
                .otherwise('Other'))
    .where(F.col('country') != 'Other')
    .withColumn('date', F.regexp_replace(F.col("date"), ",", ""))
    .withColumn('date', F.to_timestamp(F.col('date'), 'yyyy/MM/dd HH:mm:ss').cast('date'))
    .withColumn('date', F.to_date('date'))
    .groupby(['country', 'date'])
    .agg(F.sum('likeCount').alias('likes'))
    .sort('country', 'date')
    )
    
    df_player = (
    df_wordcup.filter(F.col('lang') == 'en')
    .withColumn('player', 
        F.when(
            F.col('content').like('%Messi%'), 'Messi')
            .when(F.col('content').like('%Mbappe%'), 'Mbappe')
            .when(F.col('content').like('%Lewandowski%'), 'Lewandowski')
            .when(F.col('content').like('%Modric%'), 'Modric')
            .when(F.col('content').like('%Neymar%'), 'Neymar')
            .when(F.col('content').like('%Kroos%'), 'Kroos')
            .when(F.col('content').like('%Ronaldo%'), 'Ronaldo')
                .otherwise('Other'))
    .where(F.col('player') != 'Other')
    .withColumn('date', F.regexp_replace(F.col("date"), ",", ""))
    .withColumn('date', F.to_timestamp(F.col('date'), 'yyyy/MM/dd HH:mm:ss').cast('date'))
    .withColumn('date', F.to_date('date'))
    .groupby(['player', 'date'])
    .agg(F.sum('likeCount').alias('likes'))
    .sort('player', 'date')
    )
    
    df_win = (
    df_wordcup.filter(F.col('lang') == 'en')
    .withColumn('win', 
        F.when(
            F.col('content').like('%Argentina%') | F.col('content').like('%win%'), 'Argentina win')
            .when(F.col('content').like('%France%') | F.col('content').like('%win%'), 'France win')
                .otherwise('Other'))
    .where(F.col('win') != 'Other')
    .withColumn('date', F.regexp_replace(F.col("date"), ",", ""))
    .withColumn('date', F.to_timestamp(F.col('date'), 'yyyy/MM/dd HH:mm:ss').cast('date'))
    .withColumn('date', F.to_date('date'))
    .groupby(['win', 'date'])
    .agg(F.sum('likeCount').alias('likes'))
    .sort('win', 'date')
    )
    
    return df_lang, df_country, df_player, df_win

def create_tables_and_put(df_country_pd, df_player_pd, df_win_pd, df_lang_pd):
    con = hb.Connection("localhost")
    con.open()

    if b"country_likes" not in con.tables():
        con.create_table("country_likes", {"country_info":dict()})

    if b"player_likes" not in con.tables():
        con.create_table("player_likes", {"player_info":dict()})

    if b"country_win" not in con.tables():
        con.create_table("country_win", {"country_win_info":dict()})

    if b"lang_stats" not in con.tables():
        con.create_table("lang_stats", {"lang_stats_info":dict()})


    table_country = con.table(b'country_likes')
    table_player = con.table(b'player_likes')
    table_country_win = con.table(b'country_win')
    table_lang_stats = con.table(b'lang_stats')

    for i in range(len(df_country_pd)):
        table_country.put(str(df_country_pd.index[i]),
        {b"country_info: country":str(df_country_pd.loc[i, "country"]),
        b"country_info: date":str(df_country_pd.loc[i, "date"]),
        b"country_info: likes":str(df_country_pd.loc[i, "likes"])
        })

    for i in range(len(df_player_pd)):
        table_player.put(str(df_player_pd.index[i]),
        {b"player_info: player":str(df_player_pd.loc[i, "player"]),
        b"player_info: date":str(df_player_pd.loc[i, "date"]),
        b"player_info: likes":str(df_player_pd.loc[i, "likes"])
        })

    for i in range(len(df_win_pd)):
        table_country_win.put(str(df_win_pd.index[i]),
        {b"country_win_info: win":str(df_win_pd.loc[i, "win"]),
        b"country_win_info: date":str(df_win_pd.loc[i, "date"]),
        b"country_win_info: likes":str(df_win_pd.loc[i, "likes"])
        })

    for i in range(len(df_lang_pd)):
        table_lang_stats.put(str(df_lang_pd.index[i]),
        {b"lang_stats_info: lang":str(df_lang_pd.loc[i, "lang"]),
        b"lang_stats_info: count_lang":str(df_lang_pd.loc[i, "count_lang"]),
        b"lang_stats_info: total_likes_lang":str(df_lang_pd.loc[i, "total_likes_lang"]),
        b"lang_stats_info: mean_likes_lang":str(df_lang_pd.loc[i, "mean_likes_lang"]),
        b"lang_stats_info: max_likes_lang":str(df_lang_pd.loc[i, "max_likes_lang"])
        })
        
    con.close()

def main():
    df_lang, df_country, df_player, df_win = process_pyspark()
    
    df_lang_pd = df_lang.toPandas()
    df_country_pd = df_country.toPandas()
    df_player_pd = df_player.toPandas()
    df_win_pd = df_win.toPandas()
    
    create_tables_and_put(df_country_pd, df_player_pd, df_win_pd, df_lang_pd)
    
if __name__ == "__main__":
    main()

