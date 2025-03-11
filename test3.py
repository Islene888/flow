from pyspark.sql import SparkSession
import random
import os
os.environ['JAVA_HOME'] = "/opt/module/java/jdk1.8.0_401"


# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("NumberProcessing") \
    .getOrCreate()

# 生成一个包含随机整数的 RDD
data = [random.randint(1, 100) for _ in range(100)]
rdd = spark.sparkContext.parallelize(data)

# 计算平均值
average = rdd.mean()

# 计算最大值
maximum = rdd.max()

# 计算最小值
minimum = rdd.min()

# 输出结果
print(f"Generated Numbers: {data}")
print(f"Average: {average}")
print(f"Maximum: {maximum}")
print(f"Minimum: {minimum}")


# 停止 SparkSession
spark.stop()