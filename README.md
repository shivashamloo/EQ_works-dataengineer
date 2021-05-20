
# Solution for the Work Sample for Data Aspect, PySpark Variant

This is the solution of [EQ works take home assessment?](https://gist.github.com/woozyking/f1d50e1fe1b3bf52e3748bc280cf941f#file-question-txt)

The program is in solution.py and the jupyter notebook is in solution.ipynb

Solution for question 1,2,3,4a are written in spark
Solution for question 4b is written in pandas

## Setup

Follow the Environment setup of the assessment. Afterwards, please install geopy using the following command:

```
pip install geopy
```

## Solutions

### 1. Cleanup
The originial dataset contains 22025 record.After removing the suspicious records, it has 17973 records.

### 2. Label
Assigning each record to the closest POI. Since POI1 and POI2 has the same location, there are points that assigned to both POI1 and POI2. Furthur information is needed to assign each of these records to either POI1 or POI2.

### 3.Analysis

|POIID|average|standard deviation|radius|requests|density|
|POI4|1287.0372141876474|268.382193451716| 8899.676501173562|7089|2.811812828200878E-4|
|POI2| 538.5734471190653|988.328870276214| 3704.889111069485|6307|0.001443518240010...|
|POI1| 538.5734471190653|988.328870276214| 3704.889111069485|6307|0.001443518240010...|
|POI3|2115.8017213703793|1355.6171574764228|3959.5162091581888|4577|9.171630180694846E-4|



### 4.Data Science/Engineering Tracks

#### 4a.Model
The popularity is based on hyperbolic tangent function 
`Popularity = 10*((e^x-e^-x)/(e^x+e^-x))`
Where 
`x = 1-(average/std)+(count/total)`
This formula is based on the fact that:
* The higher the average distance, the less popular the POI.
* Standard deviation measures the amount of dispersion, as a result, the higher the std, the less effect of average.
* The more percentage of the records belong to the certain POI the more popular the POI 

|POIID|average|standard deviation|radius|requests|density|popularity|
|POI4|1287.0372141876474|268.382193451716| 8899.676501173562|7089|2.811812828200878E-4|-9.981908496237493|
|POI2|538.5734471190653|988.328870276214| 3704.889111069485|6307|0.001443518240010...|6.136952250611687|
|POI1|538.5734471190653|988.328870276214|3704.889111069485|6307|0.001443518240010...| 6.136952250611687|
|POI3|2115.8017213703793|1355.6171574764228|3959.5162091581888|4577|9.171630180694846E-4|-3.559646111231005|
### 4b.Pipeline Dependency
Finds solution based on Breadth First Search.
`[21, 73, 100, 20, 73, 112, 73, 94, 73, 56, 97, 102, 36]` 

## Author

Shiva Shamloo
