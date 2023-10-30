import sys
import requests
import csv
import calendar
from datetime import date, datetime
import clickhouse_connect

from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD


# TODO: Check OS system?

# Establish a connection to ClickHouse
client = clickhouse_connect.get_client(host=DB_HOST, username=DB_USER)


def retrieve_data():
    """
    Retrieve data from the provided URL, process it, and insert it into the database.
    """
    client.command(drop_table_sql)

    # Fetch CSV data
    csv_url = 'https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/powerreactorstatusforlast365days.txt'
    response = requests.get(csv_url)

    # Handle failed requests
    if response.status_code != 200:
        print(f"Failed to fetch CSV data from {csv_url}. Status code: {response.status_code}")
        sys.exit()

    csv_data = response.text.splitlines()

    # Create the ClickHouse table
    client.command(create_table_sql)

    # Parse and process the CSV data
    reader = csv.reader(csv_data, delimiter='|', quotechar='"')
    header = next(reader)

    # Validate header columns
    if header != ['ReportDt', 'Unit', 'Power']:
        print(f"Expected header: ['ReportDt', 'Unit', 'Power']")
        print(f"Actual header: {header}")
        sys.exit()

    # Convert rows and bulk insert
    rows_to_insert = process_rows(reader)
    bulk_insert(rows_to_insert)


def process_rows(reader):
    """
    Convert rows to proper format and add them to a bulk insert list.
    """
    rows_to_insert = []
    for row in reader:
        if row == [' ']:
            continue
        if len(row) < 3:
            print(f"Skipping incomplete row: {row}")
            continue
        try:
            formatted_date = datetime.strptime(row[0], '%m/%d/%Y %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S')
            rows_to_insert.append((formatted_date, row[1], int(row[2])))
        except Exception as e:
            print(f"Error processing row: {row}. Error: {e}")
    return rows_to_insert


def bulk_insert(rows_to_insert):
    """
    Insert given rows into the database.
    """
    if not rows_to_insert:
        print("\nNo valid rows to insert.\n")
        main_menu()
        return

    insert_query = insert_data_sql
    values_str_list = [f"('{row[0]}', '{row[1]}', {row[2]})" for row in rows_to_insert]
    insert_query += ", ".join(values_str_list)

    try:
        client.command(insert_query)
        print("\nData updated successfully!\n")
        main_menu()
    except Exception as e:
        print(f"Error during insertion: {e}")


def list_all_reactors():
    """
    List all available reactors in the database.
    """
    result = client.command(select_all_reactors_sql)
    print("\n" + result + "\n")


def list_reactor_outage_by_date():
    """List reactors with outage within a specified date range."""

    def fetch_date(order_by_clause):
        query = f"SELECT DATE(mt.ReportDt) FROM my_table mt ORDER BY {order_by_clause} LIMIT 1;"
        date_str = client.command(query)
        return datetime.strptime(date_str, '%Y-%m-%d')

    min_date = fetch_date("mt.ReportDt")
    formatted_date_min = min_date.strftime('%m/%d/%Y')

    max_date = fetch_date("mt.ReportDt DESC")
    formatted_date_max = max_date.strftime('%m/%d/%Y')

    start_date = input_date(f"Enter the start date between {formatted_date_min} and {formatted_date_max}")
    formatted_date_start = start_date.strftime('%m/%d/%Y')

    end_date = input_date(f"Enter the end date between {formatted_date_start} and {formatted_date_max}")
    formatted_date_end = end_date.strftime('%m/%d/%Y')

    query = f"SELECT DISTINCT Unit, CAST(COUNT(*) AS VARCHAR) FROM my_table WHERE Power = 0 AND ReportDt BETWEEN DATE('{start_date}') AND DATE('{end_date}') GROUP BY Unit;"
    result = client.command(query)

    separator = "           Number of days down: "
    result_string = separator.join(result)

    print("\n" + "*" * 40)
    print(f"\nReactors that are down between {formatted_date_start} and {formatted_date_end}:\n")
    print(result_string)
    print("\n" + "*" * 40)


def get_year(minYear, maxYear):
    while True:
        year_input = input("Year: ")
        if not year_input:
            return None

        try:
            year = int(year_input)
            if minYear <= year <= maxYear:
                return year
            print(f"Please input a year between {minYear} and {maxYear}.")
        except ValueError:
            print("Please enter a valid year.")


def get_month():
    while True:
        month_input = input("Month (1 - 12): ")
        if not month_input:
            return None

        try:
            month_int = int(month_input)
            if 1 <= month_int <= 12:
                return month_int
            print("Please input a month between 1 and 12.")
        except ValueError:
            print("Please enter a valid month number.")


def get_day(year, month):
    num_of_days = calendar.monthrange(year, month)[1]
    while True:
        day_input = input(f"Day (1 - {num_of_days}): ")
        if not day_input:
            return None

        try:
            day_int = int(day_input)
            if 1 <= day_int <= num_of_days:
                return day_int
            print(f"Please input a day between 1 and {num_of_days}.")
        except ValueError:
            print("Please enter a valid day number.")


def confirm_date(year, month, day):
    while True:
        print(f"Confirm Date of {month}/{day}/{year}: y or n")
        conf = input(">>> ").lower()
        if conf in ['', 'y', 'n']:
            return conf
        print("Invalid input. Please enter 'y' or 'n'.")


def input_date(prompt_text):
    query = "SELECT MIN(EXTRACT(YEAR FROM ReportDt)), MAX(EXTRACT(YEAR FROM ReportDt)) FROM my_table mt;"
    minYear, maxYear = [int(x) for x in client.command(query)]

    print(prompt_text)

    year = get_year(minYear, maxYear)
    if year is None: return None

    month = get_month()
    if month is None: return None

    day = get_day(year, month)
    if day is None: return None

    confirmation = confirm_date(year, month, day)
    if confirmation == 'n':
        return input_date(prompt_text)
    elif confirmation == '':
        return None

    return date(year, month, day)


def reactor_info():
    """
    Retrieve and display information about a specified reactor.
    """
    print("\nEnter the name of the reactor for which you need information (Press Enter to Return):")

    user_input = input(">>> ")
    if user_input == '':
        return
    # TODO: Input search history?

    print("\n" + "*" * 40)

    # Check reactor name
    query = f"SELECT DISTINCT Unit FROM my_table WHERE Unit = '{user_input}'"  # Possible injecting risk
    reactorName = client.command(query)
    query = f"SELECT DISTINCT * FROM my_table mt WHERE Unit = '{user_input}' AND ReportDt <= DATE(NOW()) ORDER BY ReportDt DESC LIMIT 1;"  # Possible injecting risk
    reactorStatus = client.command(query)
    reactorPower = int(reactorStatus[2])  # For the power number comparison.
    query = f"SELECT COUNT(Unit) FROM my_table WHERE my_table.Unit = '{user_input}' AND my_table.Power = 0;"  # Possible injecting risk
    downNumber = client.command(query)
    query = f"SELECT ReportDt FROM my_table mt WHERE Unit = '{user_input}' AND Power = 0 ORDER BY ReportDt DESC LIMIT 1;"
    lastDownDate = client.command(query)

    # TODO: Input exception for non existent reactor (try).
    if not user_input:
        print(f"No records found for reactor '{user_input}'.")
        return

    print(f"*** {reactorName} ***\n")

    if reactorPower == 0:
        print("This reactor is down as of " + reactorStatus[0].split()[0] + "\n")
    elif 0 < reactorPower < 50:
        print("This reactor is running low power as of " + reactorStatus[0].split()[0] + "\n")
    elif reactorPower >= 50:
        print("This reactor is running as of " + reactorStatus[0].split()[0] + "\n")

    print("Reactor power level: " + reactorStatus[2])
    print(f"Days reactor was down in the last 365 days: {downNumber}")
    print(f"Last date the reactor was down: {lastDownDate.split()[0]}")

    print("*" * 40)

    input("\nPress Enter to continue...")


def main_menu():
    """
    Display the main menu and handle user input.
    """
    while True:
        print("\nWelcome to the program:")
        print("1) List all of the Reactors")
        print("2) Retrieve Reactor Information")
        print("3) List outage reactors by date range")
        print("4) Re-Retrieve Data")
        print("5) Exit")

        user_input = input(">>> ")

        if user_input == '1':
            list_all_reactors()
        elif user_input == '2':
            reactor_info()
        elif user_input == '3':
            list_reactor_outage_by_date()
        elif user_input == '4':
            retrieve_data()
        elif user_input == '5':
            sys.exit(0)
        else:
            print("\nPlease input a correct value.\n")


if __name__ == '__main__':
    # Opening SQL files to execute.
    with open("sql/DropTable.sql", "r") as sql_file:
        drop_table_sql = sql_file.read()

    with open("sql/CreateTable.sql", "r") as sql_file:
        create_table_sql = sql_file.read()

    with open("sql/InsertData.sql", "r") as sql_file:
        insert_data_sql = sql_file.read()

    with open("sql/ListAllReactors.sql", "r") as sql_file:
        select_all_reactors_sql = sql_file.read()

    retrieve_data()
    main_menu()
