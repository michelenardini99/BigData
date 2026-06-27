# BigData

Big Data course project for the University of Bologna Big Data course.

The project analyzes United States flight delay data with Apache Spark. The goal is to compute, for each airline and origin airport location, the average arrival delay and the average delay causes. Two implementations of the same job are provided:

- non-optimized version: standard RDD join followed by aggregation
- optimized version: broadcast join for the small airports table followed by the same aggregation

Both versions produce the same final result and are intended to compare the effect of avoiding the join shuffle.

## Dataset

The project uses three CSV files:

- `flights_final.csv`: full flights dataset, about 37 GB
- `flights_sample.csv`: development sample.
- `airports.csv`: airport metadata
- `airlines.csv`: airline metadata

The full dataset is not included in the delivery archive because of its size.
The dataset can be found at the link above:
```text
https://www.kaggle.com/datasets/bojasthegreat/usdot-1987-2025-flights-dataset?select=flights_final.csv
```

## Job Description

The main job computes the following output columns:

- `Airline`
- `City`
- `State`
- `Avg_Arrival_Delay`
- `Avg_Airline_Delay`
- `Avg_Weather_Delay`
- `Avg_NAS_Delay`
- `Avg_Security_Delay`
- `Avg_Late_Aircraft_Delay`

The job keeps only flights with positive arrival delay, joins each flight with its origin airport, groups records by `(airline, city, state)`, and computes the average of each delay field.

## Implementations

The Scala application is in:

```text
src/main/scala/flights/FlightsAnalysis.scala
```

The parser is in:

```text
src/main/scala/flights/FlightsParser.scala
```

## Local Development Notebook

The notebook used for local development is:

```text
src/main/python/notebook/flyghts_analysis.ipynb
```

It uses the sample dataset for development and debugging. The Scala application is the final implementation used for AWS execution.

The jar is generated in:

```text
build/libs/BigData.jar
```

## Spark Histories

Spark event histories are included in:

```text
history/nopt/application_1781960305316_0002
history/opt/application_1781960305316_0003
```

## Output

Both versions produce the same result shape and the same number of output records:

The output CSV files are stored in:

```text
output/avgDelayNotOpt
output/avgDelayOpt
```

## Visualization

The Power BI report is included in:

```text
report/report.pbix
```

It can be used to inspect and visualize the aggregated delay results.

