
# 🛡️ OSINT System

Welcome to the **OSINT System**, a modular and extensible platform for Open Source Intelligence (OSINT) gathering and analysis. This system is designed to automate the collection, storage, and analysis of data from various sources (such as social media, domains, etc.) using a distributed agent-based architecture.

## 🚀 Features

*   **Modular Agent Architecture**: Independent, scalable agents for different tasks (Manager, Collector, Analysis, Storage).
*   **Message-Driven**: Utilizes RabbitMQ for reliable, asynchronous communication between components.
*   **Diverse Data Collection**:
    *   **Domain Intelligence**: WHOIS lookup, DNS enumeration.
    *   **Social Media**: Twitter scraping (via snscrape integration).
*   **Automated Analysis**: Automatic risk scoring and keyword detection on collected data.
*   **Secure API**: JWT-authenticated REST API for submitting tasks and retrieving results.
*   **Data Persistence**: Structured storage of raw and processed data using PostgreSQL.
*   **Interactive Dashboard**: Simple HTML/JS frontend for monitoring and interaction.
*   **Infrastructure-as-Code**: Fully dockerized dependencies (RabbitMQ, Redis, PostgreSQL).

## 🏗️ Architecture Overview

The system operates through several key components:

1.  **Manager Agent**: Initial point of contact. Decomposes high-level requests into specific collection tasks.
2.  **Collector Agents**: Specialized agents that fetch data from external sources.
3.  **Storage Agent**: Listens for data events and persists them to the database.
4.  **Analysis Agent**: Consumes raw data to perform analysis and risk assessment.
5.  **API Gateway**: Exposes endpoints for users/frontend to interact with the system.

## 📋 Prerequisites

Before you begin, ensure you have the following installed:

*   **Python 3.10+**
*   **Docker & Docker Compose**
*   **Git**

## 🛠️ Installation & Setup

1.  **Clone the Repository**
    ```bash
    git clone https://github.com/yourusername/osint-system.git
    cd osint-system
    ```

2.  **Set Up Environment**
    Create a virtual environment and install dependencies:
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    pip install -r requirements.txt
    ```

3.  **Start Infrastructure**
    Launch the required services (RabbitMQ, Redis, PostgreSQL) using Docker Compose:
    ```bash
    docker-compose up -d
    ```
    *Wait a few moments for the containers to fully initialize.*

## 🏃 Usage

You will need to run the API and the Agents simultaneously. Ideally, open two terminal windows.

### 1. Start the API Server

In the first terminal:
```bash
uvicorn osint_system.api.main:app --reload
```
The API will be available at `http://localhost:8000`.
Swagger Documentation: `http://localhost:8000/docs`

### 2. Start the Agents

In the second terminal:
```bash
python run_agents.py
```
This script launches the Manager, Collectors, Storage, and Analysis agents.

### 3. Access the Dashboard

Open `frontend/index.html` in your web browser.

*   **Default Login**:
    *   **Username**: `admin`
    *   **Password**: `admin`

From the dashboard, you can:
*   Submit new collection tasks (e.g., Target Type: `domain`, Target: `google.com`).
*   View real-time collection results.
*   See analysis outcomes and risk scores.

## 📁 Project Structure

```
osint_system/
├── agents/             # Agent implementations (Manager, Collector, etc.)
├── api/                # FastAPI application and security logic
├── core/               # Core utilities, Base Agent, Config
├── messaging/          # RabbitMQ/Kafka abstraction layer
├── storage/            # Database models and session management
└── frontend/           # HTML/JS Dashboard
```

## 🔒 Security

*   **Authentication**: The API is secured with JWT (JSON Web Tokens).
*   **Secrets**: Configuration is managed via environment variables (see `core/config.py`).
*   **Database**: All data interactions are handled securely via SQLAlchemy ORM.

## 🤝 Contributing

Contributions are welcome! Please feel free to urge a Pull Request or open an issue.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
