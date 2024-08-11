
# TBOT-WIND: AI-Powered Webhook Generation Framework

**TBOT-Wind** is an open-source framework that enables users to create Webhooks for trading indicators. With TBOT-Wind, you can implement custom indicators using Python and integrate them with OpenAI for AI-powered trading decisions. This project allows sending messages via HTTP POST to remote servers, making it highly flexible for personal and cloud deployments.

## Key Benefits of TBOT-WIND
- **Custom Indicator Development**: Create trading indicators with Python and OpenAI integration.
- **Reduced Network Delays**: Generate Webhooks locally, avoiding delays from remote platforms.
- **No Public Endpoint Required**: Maintain control and privacy by not exposing public-facing endpoints.
- **Leverage Python’s Power**: Take advantage of Python’s flexibility and rich ecosystem, especially for AI-based trading strategies.

## TBOT-Wind vs Large-Scale Platforms
While large-scale platforms excel at distributed systems, TBOT-Wind is optimized for individual users or small teams. It emphasizes:
- Real-time, low-latency data processing.
- Focus on individual needs rather than complex infrastructures.
- DataFrame-based analysis using Python’s efficient libraries.

## Historical Background
Initially developed for the TradingBoat platform, TBOT-Wind now operates independently, eliminating dependencies on external services like TradingView or Ngrok. It provides users with full control over custom technical indicators and webhook generation.

## Running TBOT-WIND

### 1. Download the Program
```
git clone https://github.com/PlusGenie/tbot-wind.git
```

### 2. Install Poetry and TBOT-WIND
If you don't have Poetry, install it:
```
pip install poetry
```

Navigate to the TBOT-Wind directory and install the dependencies:
```
cd tbot-wind
poetry install
```

### 3. Configure Environment Variables
Set up environment variables in a `.env` file to run the sample indicators:
```
cp src/tbot_wind/user/dotenv .env
```

Configure your HTTP server address:
```
TBOT_WIND_HTTP_SERVER_ADDR="http://localhost:5000/webhook"
```

### 4. Initialize Database (Optional)
If you are running TBOT-Wind for the first time, initialize the database to load historical market data:
```
poetry run tbot-wind --init-db
```

### 5. Run TBOT-Wind
Once everything is set up, run the indicators:
```
poetry run tbot-wind
```

TBOT-Wind provides a powerful, flexible solution for creating custom trading indicators and generating real-time Webhook alerts.
