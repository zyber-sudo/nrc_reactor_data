# NRC Reactor Data Ingestor & Exposer

This project ingests and exposes data from the United States Nuclear Regulatory Commission (NRC) regarding the status of
nuclear reactors over the last 365 days. The provided Python module allows users to interact with the data effectively.

### Features:

1. Retrieve all reactors.
2. Get detailed information about a specific reactor.
3. List all reactors that had an outage within a specified date range.
4. Fetch the last known outage date of a reactor.

### Prerequisites:

* Python3
* Clickhouse Database (In this case hosted locally)

### Installation & Setup:

#### Clone the repository:

1. This will clone the repository.

`git clone https://github.com/zyber-sudo/nrc_reactor_data.git
cd https://github.com/zyber-sudo/nrc_reactor_data.git`

2. This will set up the virtual environment.

`python3 -m venv venv
source venv/bin/activate`

3. This will install the proper packages.

`pip install -r requirements.txt`

#### Database Configuration:

* Install and start ClickHouse.
* Adjust the configuration in config.py to connect to your ClickHouse instance.

### Support:

For any issues or improvements, please create an issue in the repository or contact the repository owner.

### License:

This project is open source and available under the MIT License.
