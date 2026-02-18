# Apache Airflow – Docker Setup

This project contains a **Dockerized Apache Airflow environment** for orchestrating workflows and ETL pipelines.

It includes:

* `dags/` → Airflow DAGs
* `plugins/` → Custom plugins and operators
* `config/` → Configuration files
* `logs/` → Airflow logs (not committed to Git)
* `docker-compose.yaml` → Airflow services definition

---

## 📌 Project Structure

```
.
├── config/
├── dags/
├── logs/
├── plugins/
├── docker-compose.yaml
└── README.md
```

---

## 🚀 Getting Started

### 1️⃣ Prerequisites

* Docker
* Docker Compose
* Git

---

### 2️⃣ Clone the repository

```bash
git clone https://github.com/Mohamed-Amine-Dammak/airflow.git
cd airflow
```

---

### 3️⃣ Start Airflow

```bash
docker compose up -d
```

First time only (if needed):

```bash
docker compose up airflow-init
```

---

### 4️⃣ Access Airflow UI

Open your browser:

```
http://localhost:8080
```

Default credentials (if not changed):

```
Username: airflow
Password: airflow
```

---

## 📂 Adding a DAG

1. Place your DAG file inside:

```
dags/
```

2. Airflow will automatically detect it.
3. Refresh the UI and enable the DAG.

---

## 🔐 Environment Variables

If needed, create a `.env` file:

```
AIRFLOW_UID=50000
```

(Do not commit `.env` to GitHub)

---

## 🧪 Testing

To check logs:

```bash
docker compose logs -f
```

To stop services:

```bash
docker compose down
```

---

## 📊 Use Cases

This Airflow setup can be used for:

* ETL orchestration
* Triggering n8n workflows
* Triggering Talend Cloud jobs
* Monitoring external APIs
* Cloud Run integrations
* BigQuery / GCS automation
* DevOps scheduling tasks

---

## 🛠 Tech Stack

* Apache Airflow
* Docker
* Docker Compose
* Python
* REST APIs

---

## 📌 Best Practices

* Avoid heavy code at DAG top-level
* Use `@dag` decorator (Airflow 2+ / 3 style)
* Store credentials in Airflow Connections
* Use retries and email alerts
* Keep logs and secrets out of Git

---

#### 👤 Mohamed Amine Dammak, Engineering Student – Data Engineering & AI

