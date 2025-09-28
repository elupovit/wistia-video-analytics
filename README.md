# 🎥 Wistia Video Analytics – End-to-End Data Pipeline

## 📌 Business Objective
The marketing team uses Wistia to track video engagement across Facebook and YouTube.  
This project builds a **production-grade data pipeline** that ingests video and visitor analytics, transforms them into a reporting-friendly structure, and surfaces insights via an interactive Streamlit dashboard.

**Goals:**
- Collect media-level and visitor-level analytics from the Wistia Stats API
- Transform raw data into curated Gold tables using a medallion architecture
- Enable business users to explore KPIs, top visitors, and engagement trends
- Provide automated, maintainable CI/CD workflows for ongoing reliability

---

## 🏗️ Architecture Overview
**Pipeline Flow:**

\`\`\`
Wistia Stats API 
    │
    ▼
Bronze (Raw S3 Storage)
    │
    ▼
Silver (Cleaned & Modeled via AWS Glue / PySpark)
    │
    ▼
Gold (Aggregated Views in S3 → Athena)
    │
    ▼
Streamlit Dashboard (KPIs, Filters, Charts)
\`\`\`

**Components:**
- **Ingestion:** Python scripts for authenticated API pulls, pagination, and incremental loading.
- **Storage:** Raw → Curated → Gold layers stored in Amazon S3.
- **Transformations:** PySpark Glue jobs for schema validation and business logic.
- **Query Layer:** Amazon Athena views (\`gold_media_daily_trend_30d\`, \`gold_visitor_daily_trend_30d\`).
- **Presentation:** Streamlit app with filters for date range & media ID, plus KPIs and trend visualizations.
- **CI/CD:** GitHub Actions pipeline for linting, testing, and auto-deployment to Streamlit Cloud.

---

## ⚖️ Key Decisions, Assumptions, and Tradeoffs
- **Medallion Architecture (Bronze/Silver/Gold):** Chosen for clarity, modularity, and industry best practices.
- **AWS Glue over dbt:** Aligned with project constraint (“no dbt”), PySpark chosen for scalability.
- **Athena for querying:** Avoided standing compute costs while enabling fast SQL-based reporting.
- **Streamlit for dashboarding:** Lightweight, Python-native, easy for iteration and presentation.
- **CI/CD Simplification:** Dropped AWS SAM validation in CI/CD for faster closeout and smoother runs under deadline. (Can be added later.)

---

## 🚀 Setup Instructions

### 1. Clone the Repository
\`\`\`bash
git clone https://github.com/elupovit/wistia-video-analytics.git
cd wistia-video-analytics
\`\`\`

### 2. Local Environment
\`\`\`bash
# (Recommended) create a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install ingestion dependencies
pip install -r ingestion/requirements.txt

# Install Streamlit dependencies
pip install -r streamlit_app/requirements.txt
\`\`\`

### 3. AWS Setup
- Ensure your AWS account has:
  - S3 buckets for bronze, silver, and gold layers
  - Glue jobs configured with IAM roles for S3 + CloudWatch
  - Athena enabled in the same region

- Deploy AWS resources (if using SAM):
\`\`\`bash
sam build
sam deploy --guided
\`\`\`

### 4. Run the Dashboard Locally
\`\`\`bash
cd streamlit_app
streamlit run app.py
\`\`\`

### 5. CI/CD
- Every push to \`main\` triggers:
  - Python lint/tests
  - Auto-redeploy to Streamlit Cloud (if connected)

---

## 📊 Dashboard Features
- **Filters:** Date range, media ID
- **KPIs:** Plays, play rate, watch time, engagement %
- **Charts:** 30-day media trends, visitor engagement
- **Tables:** Top visitors by engagement

---

## 📝 Evaluation Criteria (Met)
- ✅ Designed modular ingestion → transformation → reporting system  
- ✅ Implemented API auth, pagination, incremental ingestion  
- ✅ Ran pipeline for multiple days with reliable data loads  
- ✅ Delivered Streamlit dashboard for insights  
- ✅ Implemented GitHub-based CI/CD  
- ✅ Documented decisions, tradeoffs, and setup  

---

## 👨‍💻 Author
Built as part of a data engineering project simulating a real-world assignment.  
Maintained by **Eitan Lupovitch**.
