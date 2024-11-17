# Data Pattern Observatory

A system for monitoring and analyzing data patterns throughout their lifecycle. This solution tracks how data is created, accessed, moved, and used within an organization, providing business context and pattern detection capabilities.

## Features

- Real-time data movement tracking
- Pattern and anomaly detection
- Usage analytics
- Business context integration
- Interactive visualizations

## Prerequisites

- Python 3.10 or newer
- Docker Desktop
- Git

## Setup Instructions

### Windows Setup

1. Install Python
   ```bash
   # Download Python from python.org
   # During installation, CHECK "Add Python to PATH"
   
   # Verify installation
   python --version
   pip --version
   ```

2. Install Docker Desktop
   - Download from [docker.com](https://www.docker.com/products/docker-desktop/)
   - Install and start Docker Desktop
   - Verify installation:
     ```bash
     docker --version
     docker-compose --version
     ```

3. Create and activate virtual environment
   ```bash
   # Create project directory
   mkdir data-observability
   cd data-observability

   # Create virtual environment
   python -m venv venv

   # Activate virtual environment
   venv\Scripts\activate

   # Upgrade pip
   python -m pip install --upgrade pip
   ```

### Unix/MacOS Setup

1. Install Python
   ```bash
   # MacOS (using Homebrew)
   brew install python

   # Linux
   sudo apt-get update
   sudo apt-get install python3 python3-pip
   ```

2. Install Docker
   - MacOS: Download Docker Desktop from [docker.com](https://www.docker.com/products/docker-desktop/)
   - Linux:
     ```bash
     sudo apt-get install docker docker-compose
     ```

3. Create and activate virtual environment
   ```bash
   # Create project directory
   mkdir data-observability
   cd data-observability

   # Create virtual environment
   python3 -m venv venv

   # Activate virtual environment
   source venv/bin/activate

   # Upgrade pip
   pip install --upgrade pip
   ```

### Common Setup Steps (All Platforms)

1. Clone repository
   ```bash
   git clone git@github.com:siddhu-pikachu/data-observability.git
   cd data-observability
   ```

2. Install required packages
   ```bash
   pip install -r requirements.txt
   ```

3. Start Docker services
   ```bash
   docker-compose up -d
   ```

4. Initialize databases
   ```bash
   python scripts/init_db.py
   ```

5. Run the application
   ```bash
   streamlit run streamlit_app.py
   ```

## Project Structure
```
data-observability/
├── src/
│   └── core/
│       ├── __init__.py
│       ├── database.py
│       └── config.py
├── scripts/
│   └── init_db.py
├── streamlit_app.py
├── requirements.txt
├── docker-compose.yml
└── README.md
```

## Configuration

The system uses the following default ports:
- Streamlit: http://localhost:8501
- MongoDB: localhost:27017
- Elasticsearch: http://localhost:9200
- Redis: localhost:6379
- TimescaleDB: localhost:5432

## Usage

After starting the application, navigate to http://localhost:8501 in your web browser. The interface provides:
- Data movement tracking
- Pattern analysis
- Usage analytics
- Real-time monitoring

## Troubleshooting

1. Port Conflicts
   ```bash
   # Windows
   netstat -ano | findstr "PORT_NUMBER"
   
   # Unix/MacOS
   lsof -i :PORT_NUMBER
   ```

2. Docker Issues
   ```bash
   # Restart containers
   docker-compose down
   docker-compose up -d
   ```

3. Package Installation Issues
   ```bash
   # If you encounter SSL errors
   pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt
   ```

## License

This project is licensed under the MIT License - see the LICENSE file for details

## Authors

- Siddhu Rapeti : sxr230189@utdallas.edu
- Abhirup Mukherjee : axm240026@utdallas.edu
- Project Link: https://github.com/siddhu-pikachu/data-observability

## Acknowledgments
The code was for this was inspired by PNC Bank's workshop demo code at Hack UTD to solve their challenge statement: As organizations increasingly rely on data to drive decision-making, ensuring a clear understanding of how data is created, accessed, moved, and used is critical for security, performance, and governance. Yet, many organizations struggle to maintain visibility into these data flows, leading to inefficiencies, security risks, and compliance challenges. We challenge you to design an innovative solution that enhances the observability of data patterns throughout its lifecycle.  Your solution should allow organizations to monitor and analyze how data is created, accessed, moved, and used – with business context.
