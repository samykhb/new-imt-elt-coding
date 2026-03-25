# KICKZ EMPIRE — ELT Pipeline

ELT (Extract, Load, Transform) pipeline for the **KICKZ EMPIRE** e-commerce website, built as part of the IMT Data Engineering course.

## 1. Project description

**KICKZ EMPIRE** is an e-commerce website specializing in **sneakers** and **streetwear** (Nike, Adidas, Jordan, New Balance, Puma). The site sells sneakers, hoodies, t-shirts, joggers, and accessories.

> We are a 3 data engineers at KICKZ EMPIRE. The Data team is asking us to build an ELT pipeline to feed analytics dashboards and recommendation models.

## 2. Architecture Diagram

```
S3 (CSV)  ──→  🥉 Bronze (raw)  ──→  🥈 Silver (clean)  ──→  🥇 Gold (analytics)
```

| Layer | Schema | Description |
|---|---|---|
| **Bronze** | `bronze_group8` | Raw data — faithful copy of CSV files from S3 |
| **Silver** | `silver_group8` | Cleaned data — internal columns removed, PII masked |
| **Gold** | `gold_group8` | Aggregated data — ready for dashboards |

## 3. Project Structure and Setup instructions 


```
├── docs/
├── src/
│   ├── __init__.py
│   ├── database.py             # PostgreSQL connection (AWS RDS)
│   ├── extract.py              # Extract: S3 (CSV) → Bronze
│   ├── transform.py            # Transform: Bronze → Silver
│   └── gold.py                 # Gold: Silver → Gold (aggregations)
├── pipeline.py                 # ELT orchestrator
├── tests/                      # Tests
├── .env.example                # Environment variables template
├── .gitignore
├── requirements.txt
└── README.md
```


```bash
# 1. Setup
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # Configure with your credentials (DB + AWS)

# 2. Test the connection
python -m src.database


```


## 4. How to run

If you want to run the whole pipeline: 

```bash
python pipeline.py
```
If you want to run a specific step of the pipeline (gold for example): 

```bash
python pipeline.py --step gold
```

## 5. How to test

```bash
pytest tests/ -v
```

If you want to generate a coverage report:

```bash
pytest tests/ -v --cov=src --cov-report=html
open htmlcov/index.html
```

## 6. Tech Stack

- **Python 3.10+** : Main language
- **pandas** : Data manipulation
- **boto3** : AWS S3 access
- **SQLAlchemy** : ORM / PostgreSQL connection
- **PostgreSQL** (AWS RDS) : Database
- **pytest** : Testing (TP3)

## 7. Team Members

KHATIB Mohamed-Taib, KHEBBEB Samy, MÉNARD PESCATORE Raphaël
