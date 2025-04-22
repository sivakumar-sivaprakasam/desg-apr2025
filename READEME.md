# Polars - Dataframe Library

### Install Polars

```python
pip install polars
```

### Usecase #1: Analysing Singapore HDB Dataset

Run the below script to get the analysis results

```python
python polars_df_eager.py
```

### Usecase #2: Comparing Synthetic Customer Dataset against Polars, Pandas & PySpark

To generate the synthetic dataset, you need to have `ficto` library installed

Then run the below command to generate the dataset with 1000 customers. You can change the parameter value to generate dataset of your choice

```python
ficto -d synthetic_customer_data_config.yaml -n 1000 -f csv
```

Once the dataset is generated, run the below command to run the data pipeline implemented using Polars

```python
python synthetic_customer_data_processing_polars.py <dataset file name>
```

Once the dataset is generated, run the below command to run the data pipeline implemented using Pandas

```python
python synthetic_customer_data_processing_pandas.py <dataset file name>
```

Once the dataset is generated, run the below command to run the data pipeline implemented using PySpark

```python
python synthetic_customer_data_processing_pyspark.py <dataset file name>
```

I ran the comparison against following datasets and here are the comparison results (Execution time in Seconds)

| Library | 1 Million | 5 Million | 10 Million | 25 Million | 50 Million |
| ------------- | ------- | ------ | ------- | ------ | ----- |
| Polars | 0.62 | 1.67 | 3.16 | 7.48 | 51.70 |
| Pandas | 1.90 | 8.71 | 17.00 | 51.25 | 134.39 |
| PySpark | 15.90 | 12.82 | 15.44 | 26.29 | 47.26 |

My local environment details as follows

```python
Windows 11
Intel i7-12th Generation
24 GB RAM

Polars v1.27.1
Pandas v2.2.0
PySpark v3.5.0
```

