# Koppal District Anemia Study Dashboard

An interactive real-time dashboard for tracking anemia study data across Area Codes and PSU Names in the Koppal District.

## Features
- **Real-time KPI Tracking**: Monitor total enrollment, hemoglobin levels, and anemia severity counts.
- **Interactive Map**: Visualize beneficiary locations and village boundaries with detailed tooltips.
- **Dynamic Charts**: Distribution analysis by beneficiary group and anemia status.
- **Beneficiary Tracking Table**: Filterable and sortable data table with conditional color-coding.

## Tech Stack
- **Dashboard Framework**: Plotly Dash
- **UI Components**: Dash Bootstrap Components
- **Data Processing**: Pandas
- **Visualization**: Plotly Graph Objects

## Installation & Setup

### For Windows Users (Recommended)
Simply download the folder and double-click the `run.bat` file. 
It will automatically:
1. Check for Python
2. Install necessary dependencies
3. Start the dashboard

### Manual Installation
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Start the local server:
   ```bash
   python app.py
   ```
The dashboard will be available at `http://localhost:8090/`.

## Data Source
The dashboard fetches data from a Google Sheets backend via a Google Apps Script URL.

## Live Deployment (Hosting)
To make this dashboard accessible via a public URL:

1. **GitHub**: Push this repository to your GitHub account.
2. **Render**:
   - Create a free account on [Render.com](https://render.com).
   - Click **New +** and select **Web Service**.
   - Connect your GitHub repository.
   - Use the following settings:
     - **Runtime**: `Python`
     - **Build Command**: `pip install -r requirements.txt`
     - **Start Command**: `gunicorn app:server`
3. Render will provide a live URL (e.g., `https://anemia-dashboard.onrender.com`).

