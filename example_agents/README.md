# Example Agents

This directory contains simple, standalone agent examples for the SWF testbed,
built upon a reusable base class.

These agents demonstrate the core interaction patterns with the `swf-monitor`
service (via its REST API) and the ActiveMQ message broker. They are designed
to be easy to run and understand, providing a clear blueprint for developing
new, production-grade agents.

The key design is in `base_agent.py`, which provides a `BaseAgent` class
that handles all common infrastructure. The specialized agents
(`example_data_agent.py`, `example_processing_agent.py`) inherit from this
base class and contain only the logic specific to their role.

## Prerequisites

- Python 3.9+
- An active `swf-monitor` instance with a running REST API.
- A running ActiveMQ broker.

## Setup

1.  **Create a virtual environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

    If you will run the EJFAT fast-processing agent (`fast_processing_ejfat.py`),
    also install its extra dependencies for reading EIC ROOT files over xrootd:
    ```bash
    pip install -r requirements-ejfat.txt
    ```

## Configuration

The agents are configured via environment variables, which are read by the
`base_agent.py` script:

- `SWF_MONITOR_URL`: The base URL for the `swf-monitor` REST API (e.g., `http://localhost:8000`).
- `SWF_API_TOKEN`: An authentication token for the REST API.
- `ACTIVEMQ_HOST`: The hostname of the ActiveMQ broker.
- `ACTIVEMQ_PORT`: The port of the ActiveMQ broker.
- `ACTIVEMQ_USER`: The username for the ActiveMQ broker.
- `ACTIVEMQ_PASSWORD`: The password for the ActiveMQ broker.

### Generating an API Token

To interact with the `swf-monitor` API, you need a token.

1.  **Create a user** (if you haven't already):
    ```bash
    # Make sure you are in the swf-testbed directory with the venv active
    python ../swf-monitor/src/manage.py createsuperuser
    ```

2.  **Generate a token for that user:**
    ```bash
    python ../swf-monitor/src/manage.py drf_create_token <your_username>
    ```

3.  **Set the environment variable:**
    ```bash
    export SWF_API_TOKEN=<your_generated_token>
    ```

## Running an Agent

To run a specific agent, execute its script directly after setting the
required environment variables:

```bash
# Example for running the Data Agent
export SWF_MONITOR_URL='http://localhost:8000'
export ACTIVEMQ_HOST='localhost'
# ... other variables

python example_data_agent.py
```
