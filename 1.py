class Solution:
    def __init__(self, num):
        self.lst=[]
        self.lst.append(num)
    def app(self, lstic:list[int]) -> list[int]:
        self.lst=self.lst+[x for x in lstic if x>0]
        return self.lst

my_s=Solution(20)
tl=[1,2,-33]
res=my_s.app(tl)

print(res)
t2=[333]
my_s.app(t2)
print(my_s.lst)
my_s.lst.append('kzkzkzk')
print(my_s.lst)





class Solution:
    def search(self, nums: list[int], target: int) -> int:
        left = 0
        right = len(nums) - 1#поставил указатели
        while left <= right:# пока левый не перепрыгнул правый
            midlle = (right + left) // 2
            if nums[midlle] == target:
                return midlle
            if nums[left] <= nums[midlle]:#если левая половина отсортирована
                if nums[left] <= target < nums[midlle]:#если искомое в левой отсортированной половине
                    right = midlle - 1 #сужаю рамки
                else:
                    left = midlle + 1
            else:
                if nums[midlle] < target <= nums[right]:#если правая половина отсортирована
                    left = midlle + 1
                else:
                    right = midlle - 1
        return -1



import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MetaRecommendationTest") \
    .master("local[*]") \
    .getOrCreate()
# make a view
users_friends.createOrReplaceTempView("v_friends")
users_pages.createOrReplaceTempView("v_pages")

query = """
select *
from v_friends
"""
# Запускаем движок Спарка по нашему SQL-запросу
result_sql_df = spark.sql(query)

import pyspark.sql.functions as f
from pyspark.sql import SparkSession


import pyspark.sql.functions as f
from pyspark.sql import SparkSession


import pyspark.sql.functions as f # импорт функций для работы с датафреймами
from pyspark.sql import SparkSession # импортирую спарсессию - без нее невозможны никакие распределенные вычисления

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("my_app")\
    .master("yarn")\
    .getOrCreate

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("my_app").
    master("yarn").
    getOrCreate()
)



facebook_reactions.createOrReplaceTempView("reactions")
facebook_posts.createOrReplaceTempView("posts")


hearts_df = facebook_reactions.filter(f.col("reaction") == "heart")
qery = """
    SELECT *
    FROM posts
    WHERE post_id IN (
                        SELECT post_id 
                        FROM reactions
                        WHERE reaction = 'heart'
                        GROUP BY post_id
                        )
"""

result_df = spark.sql(qery)

# 4. Финальный штрих для робота-проверяльщика StrataScratch
result_df.toPandas()


hearts_df=facebook_reactions.filter(f.coll('reaction')=='heart').select("post_id")
result_df = facebook_posts.join(hearts_df, on="post_id", how="left_semi")

# 4. Финальный штрих для робота-проверяльщика StrataScratch
result_df.toPandas()



import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName ("my_app")
        .master("yarn")
         .getOrCreate()
         )
path="my/path/to"
v_loans=spark.read.parquet(path)

resalt=(
    v_loans.filter((f.col("status")=="ISSUED") & (f.col("branch_city") == "Пенза"))
    .groupBy(f.col("manager_id"))
    .agg(
        f.count("loan_id").alias("total_loans"),
        f.sum("loan_amount").alias("total_amount")
        )
    .filter(f.col("total_amount") > 5000000)
    .orderBy(f.col("total_amount").desc())
    .limit(3)
)

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = (
        SparkSession.builder
        .master("yarn")
        .getOrCreate()
        )

v_operations = spark.read.parquet("my/path/to")

resalt = (
        v_operations
        .filter((f.col("status")=="SUCCESS") & (f.col("currency")=="RUB"))
        .groupBy(f.col("client_id"))
        .agg(
            f.count(f.col("operation_id")).alias("total_tx_coun"),
            f.sum( f.when( f.col("category") == 'Супермаркеты' , f.col("amount"))).alias("supermarket_amount"),
            f.sum( f.when( f.col("category") == 'Автоуслуги' , f.col("amount"))).alias("auto_amount")
            )
        .filter((f.col("supermarket_amount") > 30000) |(f.col("auto_amount") > 15000) )
        .orderBy(f.col("supermarket_amount").desc())
        .limit(5)
         )

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("yarn")
    .getOrCreate()
        )
clients_df = spark.read.parquet("path/to/clients")
cards_df = spark.read.parquet("path/to/cards_df")

resalt = (
    clients_df.filter(f.col("city")=="Пенза")
    .join(cards_df, on=(clients_df.client_id ==cards_df.holder_id), how = "inner")
    .filter(f.col("cards_df.status ")=="ACTIVE")
    .groupBy(f.col("clients_df.client_id"), f.col("clients_df.client_name"))
    .agg(f.count("card_id").alias("total_active_cards"))
    .orderBy(f.col("total_active_cards").desc())
        )


import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("yarn")
    .getOrCreate()
        )

v_users = spark.read.parquet ("path/to/users")
v_tx = spark.read.parquet ("path/to/tx")

resalt = (
        v_users.alias("u")
        .filter(f.col("u.city") == "Пенза")
        .join(v_tx.alias("t"), on = (f.col("u.client_id") == f.col("t.user_id")), how = "inner")
        .filter(f.col("t.status") == "SUCCESS")
        .groupBy(f.col("u.client_id"), f.col("u.client_name"))
        .agg(
            f.count_distinct(f.when(f.col("t.channel")== "app", f.col("t.category"))).alias("unique_app_categories"),
            f.count_distinct(f.when(f.col("t.channel")== "pos", f.col("t.category"))).alias("unique_pos_categories")
            )
        .orderBy(f.col("unique_app_categories").desc())
        )


import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("yarn")
    .getOrCreate()
)

loans_df = spark.read.parquet("path/to/loans_df")
pledges_df  = spark.read.parquet("path/to/pledges_df")

resalt= (
    loans_df.withColumn("pledge_id_new",f.col("pledge_id").cast("int")  )
    .join(pledges_df,on=(f.col("pledge_id_new") ==pledges_df.id), how = "inner")
    .filter(f.col("amount")>1000000)
    .select(f.col("loan_id"),f.col("amount"), f.col("pledge_name") )
)

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("yarn")
    .getOrCreate()
)

df= spark.read.parquet("path/to")

resalt = los_angeles_restaurant_health_inspections.filter(f.col("program_status")=="INACTIVE")


import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark= SparkSession.builder.master("yarn").appName("SalariesDifferences").getOrCreate()

resalt_df = (db_employee.join (db_dept, on = (db_employee.department_id== db_dept.id ),how = "inner")
            .filter(f.col("department").isin ("marketing", "engineering"))
            .agg(
            f.max(f.when(f.col("department")=="marketing", f.col("salary"))).alias("max_marketing"),
            f.max(f.when(f.col("department")=="engineering", f.col("salary"))).alias("max_engineering")
            )
            .withColumn("sal_dif", f.abs(f.col("max_marketing")- f.col("max_engineering")))
             .select(f.col("sal_dif"))
             )



import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql import Window
spark = (
    SparkSession.builder
    .appName("my_app")
    .master("yarn")
    .getOrCreate()
)

sf_events = spark.read.parquet("path/to/sf_events")

w = Window.partitionBy("user_id").orderBy("record_date")

resalt = (
    sf_events.dropDuplicates(["user_id","record_date"])
    .withColumn("prev", f.lag("record_date",1).over(w))
    .withColumn("lead", f.lead("record_date",1).over(w))
    .filter((f.datediff(f.col("record_date"),f.col("prev"))==1) & (f.datediff(f.col("lead"),f.col("record_date"))==1))
    .select(f.col("user_id"))
    .dropDuplicates(["user_id"])
    )

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql import Window

spark = (
    SparkSession.builder
    .appName("my_app")
    .master("yarn")
    .getOrCreate()
)

online_retail = spark.read.parquet("path/to/online_retail")

resalt_st1 = (
    online_retail.filter((f.col("quantity")>0) & (~f.col("invoiceno").startswith("C")))
    .withColumn("month", f.month(f.col("invoicedate")))
    .groupBy("month","description" )
    .agg(f.sum(f.col("quantity")*f.col("unitprice")).alias("total_sum"))
    .select("month", "description", "total_sum")
)

w = Window.partitionBy("month").orderBy(f.col("total_sum").desc())

resalt_st2 = (
    resalt_st1.withColumn("num_of_place", f.row_number().over(w))
    .filter(f.col("num_of_place")==1)
    .select("month","description")
)

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql import Window

spark = (
    SparkSession.builder
    .appName("my_app")
    .master("yarn")
    .getOrCreate()
)

fb_comments_count = spark.read.parquet("path/fb_comments_count")
fb_active_users = spark.read.parquet("path/fb_active_users")

w = Window.partitionBy("month",).orderBy(f.col("month_total").desc())

step_1 =  (
    fb_comments_count.alias("com").join(fb_active_users.alias("users"), on =(f.col("com.user_id") == f.col("users.user_id")), how = "inner" )
    .filter(f.col("created_at").between("2019-12-01", "2020-01-31"))
    .withColumn("month",f.month("created_at") )
    .groupBy("country", f.col("month"))
    .agg(f.sum("number_of_comments").alias("month_total"))
    .withColumn("country_rank",f.dense_rank().over(w))
)

dec = step_1.filter(f.col("month")==12).select("country", f.col("country_rank").alias("dec_rank"))
jan = step_1.filter(f.col("month")==1).select("country", f.col("country_rank").alias("jan_rank"))

step_2= (
    dec.alias("d").join(jan.alias("j"), on =(f.col("d.country")==f.col("j.country"))& ((f.col("dec_rank") > f.col("jan_rank"))), how="inner")
    .select (f.col("d.country"))
         )
step_2.write.mode("overwrite").format("parquet").save("hdfs://path/to/the/moon")

step_2.write.mode("overwrite").format("parquet").option("path", "hdfs:/path/to/the/far_avay/moon").saveAsTable("step_2")

def find_common_clients(list_1, list_2):
    set_1=set(list_1)
    inter_set = set()
    for i in list_2:
        if i  in set_1:
            inter_set.add(i)
    list_new = list(inter_set)
    return list_new

clients_system_A = [10, 20, 30, 20, 40, 50, 10]
clients_system_B = [20, 30, 60, 20, 70]

print(find_common_clients(clients_system_A, clients_system_B))


import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql import Window

spark = (
    SparkSession.builder
    .appName("my_app")
    .getOrCreate()
)

ms_employee_salary= spark.read.parquet("hdfs:// path/to/ms_employee_salary")

w = Window.partitionBy("id").orderBy(f.col("salary").desc())

resalt = (
ms_employee_salary.withColumn("best_salary", f.row_number().over(w))
.filter(f.col("best_salary") == 1)
.select("id","first_name","last_name","department_id", f.col("salary").alias("current_salary") )
)

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark= (
    SparkSession.builder
    .appName("my_app")
    .getOrCreate()
)

oscar_nominees = spark.read.parquet("hdfs:// path/to/oscar_nominees")

reasalt = (
    oscar_nominees.filter(f.col("nominee")=="Abigail Breslin")
    .groupBy("nominee")
    .agg(f.count_distinct("movie").alias("unic_movie"))
    .select(f.col("unic_movie"))
)

reasalt.write.mode("overwrite").format("parquet").save ("hdfs://path/to")
reasalt.write.mode("overwrite").format("parquet").option("path","hdfs://path", "path/to_2").saveAsTable("ansew")


import pyspark.sql.functions as f
from pypark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("my_app")
    .getOrCreate()
)

winemag_p1=spark.read.parquet("hdfs://path/to")

resalt= (
    winemag_p1.filter(f.col("description").lower().contains("plum") | f.col("description").lower().contains("cherry")|f.col("description ").lower().contains ("rose" ))
    .select("winery").distinct()
        )
resalt.write.mode("overwrite").format("parquet").save("hdfs:// path/to")
resalt.write.mode("overwrite").format("parquet").option("path", "hdfs://path/to").saveAsTable("resalt")

def valid_str(s):
    list_of_skobki=[]
    dict_of_skobki = {")":"(","]":"[", "}":"{" }
    if s[0] in [")","]", "}"]:
        return False
    for i in s:
        if i in ["(","[", "{"]:
            list_of_skobki.append(i)
        elif i in [")","]", "}"]:
            if len(list_of_skobki)==0:
                return False
            if list_of_skobki[-1] == dict_of_skobki[i]:
                list_of_skobki.pop()
            else:
                return False
        else:
            pass
    if len(list_of_skobki)==0:
        return True
    else:
        return False


import pyspark.sql.function as f
from pyspark.sql import SparkSession
from pyspark.sql import Window

spark= (
    SparkSession.builder
    .appName("my_app")
    .getOrCreate()
    )

amazon_purchases = spark.read.parquet("hdfs://path/to")


w2= Window.orderby("month").rowsBetween(-2,0)

total_of_month = (
    amazon_purchases.withColumn("month", f.month(f.col("created_at")))
    .groupBy(f.col("month"))
    .agg(f.sum("purchase_amt").alias("total_month"))
    .withColumn("averege_for_3_month", f.avg(f.col("total_month")).over(w2))
    .withColumn("number_of_month",f.row_number().over(Window.orderBy("month")))
    .filter(f.col("number_of_month") > 2)
    .select(f.col("month"), f.col("averege_for_3_month"))
                    )

total_of_month.write.mode("overwrite").format("parquet").save("hdfs://path/to")
total_of_month.write.mode("overwrite").format("parquet").option("path", "hdfs://path/to").saveAsTable("amazon_purchases")

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql import Window

spark = SparkSession.builder.appName("my_app").getOrCreate()

actor_rating_shift  = spark.read.parquet("hdfs://path/to/actor_rating_shift")



w= Window.partitionBy("actor_name").orderBy("release_date")

resalt = (
    actor_rating_shift.withColumn("previous_rating", f.lag(f.col("rating"),1).over(w))
    .withColumn("rating_difference", f.col("rating") - f.col("previous_rating"))
    .withColumn("abs_rating_difference", f.abs(f.col("rating_difference")))
    .select("actor_name",
        "movie_title",
        "release_date",
        f.col("rating").alias("current_rating"),
        "previous_rating",
        "rating_difference",
        "abs_rating_difference")
          )


resalt.write.mode("overwrite").format("parquet").save("hdfs://path/to/resalt")
resalt.write.mode("overwrite").format("parquet").option("path", "hdfs://path/to/resalt_2").saveAsTable("resalt_2")



import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql import Window
git init

spark = (
    SparkSession.builder
    .appName("my_app")
    .getOrCreate()
)


players_results = spark.read.parquet("hdfs://path/to/players_results")


w=Window.partitionBy("player_id").orderBy("match_date")

resalt_st1 = (
    players_results.withColumn("glob_num", f.row_number().over(w))
    .filter(f.col("result")=="W")
    .withColumn("win_num", f.row_number().over(w))
    .withColumn("dif", f.col("glob_num") - f.col("win_num"))
    .groupBy("player_id","dif")
    .agg(f.count(f.col("dif")).alias("streak_length"))
)

w2=Window.orderBy(f.col("streak_length").desc())

resalt_st2 = (
    resalt_st1.withColumn("max_win",f.dense_rank().over(w2))
    .filter(f.col("max_win")==1)
    .select("player_id","streak_length")
    .orderBy("player_id")
)