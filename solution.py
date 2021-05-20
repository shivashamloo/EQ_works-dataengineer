import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import Window
from geopy.distance import geodesic
from math import pi, exp
import pandas as pd


def cleanup(datasample):
    """
    1 - Cleanup
    Get DataSample
    Return Dataframe without the suspicious records
    """
    data = spark.read.options(
        header='True',
        inferSchema='True',
        delimiter=',',
    ).csv(os.path.expanduser(datasample))

    # add a column that counts the number of occurance of a record based on timeSt, Latitude and Longitude.
    data_clean = data.withColumn('repeat', f.count('*').over(Window.partitionBy([' timeSt', 'Latitude', 'Longitude'])))
    # Delete the requests that occured more than once.
    data_clean = data_clean.filter(data_clean['repeat'] == 1)
    # Delete the column that saved the occurance of a request
    data_clean = data_clean.drop('repeat')
    return data_clean


def dist(lat1, long1, lat2, long2):
    """
    Calculates the distance of two points using Geodesic Distance.

    Parameters:
    lat1: Latitude of point 1
    long1: Longitude of point 1
    lat2: Latitude of point 2
    long2: Longitude of point 2
    Return distance
    """
    return geodesic((lat1, long1), (lat2, long2)).kilometers


def label(datasample, poilist):
    """
    2-Label
    Labeling each request with the closest POI.
    Parameters:
    datasample: Dataframe of requests without the suspicious records
    poilist: POIList
    return Dataframe of labeled records
    """
    points = spark.read.options(
        header='True',
        inferSchema='True',
        delimiter=',',
    ).csv(os.path.expanduser(poilist))

    points = points.withColumnRenamed(' Latitude', 'point_lat').withColumnRenamed('Longitude', 'point_long')
    data_points = datasample.crossJoin(points)

    distance = f.udf(dist)
    # Add a column to save the distance between each request to the POIs.
    data_points = data_points.withColumn('distance', distance(data_points['Latitude'], data_points['Longitude'], \
                                                              data_points['point_Lat'], data_points['point_Long']))
    # Find the colsest POI for each request.
    data_points = data_points.withColumn('POI_distance', f.min('distance').over(Window.partitionBy('_ID')))
    # Label each request with the closest POI
    # If there are two POI closest, keep both of the labels untill further information
    data_points = data_points.filter(data_points['distance'] == data_points['POI_distance'])
    return data_points


def analysis(data_points):
    """
    3-Analysis
    Analyising the requests assigned to each POI
    Parameters:
    data_points: Dataframe of labeled requests
    Returns Dataframe of average, standard deviation, radius and density of requests assigned to each POI
    """
    points_analysis = data_points.groupBy('POIID').agg(f.avg('distance').alias('average'), \
                                                       f.stddev('distance').alias('standard deviation'), \
                                                       f.max('distance').alias('radius'), \
                                                       f.count('distance').alias('requests'))

    points_analysis = points_analysis.withColumn('density',
                                                 points_analysis['requests'] / (points_analysis['radius'] ** 2) * pi)
    return points_analysis


def tan(average, count, std):
    """
    Calculates Hyperbolic tangent based on the input
    parameters:
    average: average distance between the POI to each of its assigned requests
    Count: number of assigned requests to each POI
    std: standard deviation of the distance between the POI to each of its assigned requests
    Returns a value between -10 and 10
    """

    x = 1 - (average / std) + (count / 24820)
    return 10 * (exp(x) - exp(-x)) / (exp(x) + exp(-x))


def model_popularity(points_analysis):
    """
    4a. Model
    Calculates the popularity of a POI based on the tan function defined above
    parameters:
    points_analysis: Dataframe of average, standard deviation, radius and density of requests assigned to each POI
    Returns the input dataframe with addition of one column that consists the popularity of each POI
    """
    tan_h = f.udf(tan)

    points_analysis = points_analysis.withColumn('popularity',
                                                 tan_h(points_analysis['average'], points_analysis['requests'],
                                                       points_analysis['standard deviation']))
    return points_analysis


def pipeline_dependency(relations_input, tasks_input, question_input):
    """
    4b. Pipeline Dependency
    Determines the path of tasks that the pipeline should take
    parameters:
    relations_input: input file of relations
    tasks_input: input file of task ids
    question_input: input file of start and goal task
    Returns the path of required task from start to goal
    """
    relations = pd.read_csv(relations_input, sep='->', header=None, names=['head', 'tail'], engine='python')
    relations = relations.groupby('tail')['head'].apply(list)

    tasks = {}
    with open(tasks_input, mode='r') as file:
        ids = file.readline().split(',')
        for i in ids:
            tasks[int(i)] = False

    with open(question_input, mode='r') as file:
        start = int(file.readline().split(':')[1].strip())
        end = int(file.readline().split(':')[1].strip())

    relations = relations.to_dict()

    tasks = mark_complete(relations, start, tasks)
    return bfs(relations, start, end, tasks)


def mark_complete(graph, root, ids):
    """
    mark the tasks that where required to be completed for the start task
    parameters:
    graph: dictionary of relations where each key is a task and its value is the array of its prequisition tasks
    root: Start task
    ids: All of the tasks assuming that none are completed
    Returns all of the tasks with the information of whether they are completed or not
    """
    queue = [root]
    ids[root] = True
    while queue:
        s = queue.pop(0)
        if s in graph.keys():
            for node in graph[s]:
                if not ids[node]:
                    queue.append(node)
                ids[node] = True
    return ids


def bfs(graph, root, leaf, ids):
    """
    Finding the best path between two tasks using Breadth First Search
    parameters:
        graph: dictionary of relations where each key is a task and its value is the array of its prequisition tasks
    root: Start task
    leaf: goal task
    ids: All of the tasks and their state of completion
    """
    queue = [leaf]
    ids[leaf] = True
    path = []
    while queue:
        s = queue.pop(0)
        path.append(s)
        if s in graph.keys():
            for node in graph[s]:
                if not ids[node]:
                    queue.append(node)
                if ids[node]:
                    if node not in path:
                        path.append(root)
                ids[node] = True

    return path[::-1]


if __name__ == '__main__':
    spark = SparkSession.builder.master('local').getOrCreate()

    data_clean = cleanup('data/DataSample.csv')

    data_points = label(data_clean, 'data/POIList.csv')

    points_analysis = analysis(data_points)

    popularity = model_popularity(points_analysis)

    pipeline = pipeline_dependency('relations.txt', 'task_ids.txt', 'question.txt')










