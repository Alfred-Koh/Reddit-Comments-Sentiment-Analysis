# Reddit Comments Sentiment Analysis

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](#license) [![Status](https://img.shields.io/badge/Status-In%20Progress-orange.svg)](#)

An end-to-end Big Data pipeline for large-scale **sentiment analysis on Reddit comments** using Apache Spark, Hadoop MapReduce, and Kafka. We process ~45 million comments (~7 GB) from subreddits including `r/worldnews`, `r/technology`, and `r/science`, sourced from the [Pushshift Reddit Archive](https://files.pushshift.io/reddit/comments/).

---

## 🚀 Key Features

* **Hybrid Architecture**: Hadoop HDFS for distributed storage → Kafka for real-time streaming simulation → MapReduce for batch word-frequency jobs → Spark for fast in-memory NLP and ML.
* **Modular & Extensible**: Clean separation of ingestion, preprocessing, MapReduce, Spark EDA, and MLlib sentiment classification.
* **Automated Workflows**: Shell scripts to configure, start, and run the full pipeline end-to-end.
* **Rich Insights**: Sentiment distribution per subreddit, controversiality patterns, temporal trends, word frequency by sentiment class, and review-length bias detection.
* **ML Pipeline**: Phase 1 — TF-IDF + Logistic Regression; Phase 2 — Word2Vec + Gradient Boosted Trees (Spark MLlib).

---

## 📂 Repository Structure

```
reddit-sentiment/
├── conf/
│   ├── hadoop/              # core-site, hdfs-site, mapred-site, yarn-site XMLs
│   ├── kafka/               # Kafka server and topic configs
│   └── spark/               # spark-defaults.conf
├── data/
│   ├── sample/              # Small sample JSON for local testing
│   ├── spark_results/       # Output from Spark analysis jobs
│   └── mapreduce_results/   # Output from MapReduce jobs
├── notebooks/
│   ├── 01_EDA.ipynb
│   └── 02_model_eval.ipynb
├── scripts/
│   ├── configure_hadoop.sh
│   ├── setup_kafka.sh
│   ├── start_services.sh
│   ├── run_preprocessing.sh
│   ├── run_validation.sh
│   ├── run_kafka_stream.sh
│   └── spark/
│       ├── run_spark_analysis.sh
│       └── run_ml_pipeline.sh
├── src/
│   ├── preprocessing/
│   │   ├── data_acquisition.py
│   │   ├── data_preprocessing.py
│   │   ├── data_validation.py
│   │   └── verify_pipeline.py
│   ├── hdfs/
│   │   ├── hdfs_upload.py
│   │   └── hdfs_utils.py
│   ├── mapreduce/
│   │   ├── mapper.py
│   │   ├── reducer.py
│   │   ├── combiner.py
│   │   ├── sentiment_mapper.py
│   │   ├── sentiment_reducer.py
│   │   ├── top_mapper.py
│   │   ├── top_reducer.py
│   │   └── mapreduce_helper.py
│   ├── streaming/
│   │   ├── kafka_producer.py
│   │   └── kafka_consumer.py
│   └── spark/
│       ├── jobs/
│       │   ├── run_analysis.py
│       │   └── run_ml_pipeline.py
│       ├── utils/
│       │   ├── spark_session.py
│       │   ├── data_loader.py
│       │   └── visualization_helper.py
│       └── optimization/
│           └── spark_optimizer.py
├── test_mapreduce.py
├── run_mapreduce_jobs.sh
├── requirements.txt
└── README.md
```

---

## 📦 Dataset

| Property    | Details                                                            |
|-------------|--------------------------------------------------------------------|
| Source      | Pushshift.io Reddit Archive                                        |
| Full corpus | ~500 GB+ (all subreddits, all time)                               |
| Our slice   | ~7 GB — r/worldnews, r/technology, r/science (2015–2020)          |
| Records     | ~45 million comments after filtering                               |
| Format      | Newline-delimited JSON, `.zst` compressed                          |
| Key fields  | `body`, `score`, `author`, `subreddit`, `created_utc`, `controversiality`, `gilded` |

```bash
# Download data by subreddit and year
python src/preprocessing/data_acquisition.py \
  --subreddits worldnews technology science \
  --years 2015 2016 2017 2018 2019 2020 \
  --output data/raw/
```

---

## 🔧 Setup

```bash
# 1. Clone & install
git clone https://github.com/your-team/reddit-sentiment-bigdata.git
cd reddit-sentiment-bigdata
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Configure & start Hadoop
bash scripts/configure_hadoop.sh
bash scripts/start_services.sh

# 3. Upload data to HDFS
python src/hdfs/hdfs_upload.py

# 4. Setup Kafka
bash scripts/setup_kafka.sh
```

---

## ⚙️ Usage

```bash
# Preprocess & validate
bash scripts/run_preprocessing.sh
bash scripts/run_validation.sh

# Kafka streaming simulation
bash scripts/run_kafka_stream.sh

# MapReduce word frequency jobs
bash run_mapreduce_jobs.sh

# Spark EDA + ML
bash scripts/spark/run_spark_analysis.sh
bash scripts/spark/run_ml_pipeline.sh
```

---

## 📊 Results — Phase 1 (TF-IDF + Logistic Regression)

| Metric      | Score |
|-------------|-------|
| Accuracy    | 0.783 |
| Weighted F1 | 0.761 |
| Precision   | 0.774 |
| Recall      | 0.783 |

---

## Team Contributors  

  Alfred Koh, Bhavyasree Kondi, Bhoomika Lnu, Dhruvkumar Kamleshbhai Patel  
    