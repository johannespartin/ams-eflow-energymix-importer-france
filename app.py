import datetime
import json
from pickletools import read_stringnl
from venv import create
import requests
import xml.etree.ElementTree as ET
from typing import Dict, List
import time
import boto3
from botocore.config import Config

DATABASE_NAME = "energy-mix-database"
TABLE_NAME = "energy-mix-readings-1"

ENERGY_TYPES = {
    "Nucléaire": "nuclear",
    "Charbon": "coal",
    "Gaz": "gas",
    "Fioul": "heavy-oil",
    "Pointe": "oil",
    "Fioul + Pointe": "oil",
    "Hydraulique": "hydro",
    "Eolien": "wind",
    "Solde": "others",
    "Autres": "others",
    "Pompage": "pumped-storage",  # negative (being stored)
    "Solaire": "solar",
    "Consommation": "consumption"
}


def get_unix_timestamp(date_str: str, period: int) -> int:
    """
    Convert a string of the form "2022-10-27" and an integer corresponding to the
    15-minute time interval to a unix timestamp.
    This function assumes does not consider time shifts and assumes the given time zone is utc.
    """
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    return int((date + datetime.timedelta(minutes=period * 15)).timestamp())


def parse_xml_day(day_element: ET.Element) -> List[Dict]:
    """
    Note: will insert 0 if API says 'ND'
    """
    result = [
        {
            "time": get_unix_timestamp(day_element.attrib["date"], period),
            "country": "FR"
        }
        for period in range(len(day_element[0]))  # current length of the day
    ]
    for energy_type_element in day_element:
        for period_element in energy_type_element:
            energy_type = ENERGY_TYPES[energy_type_element.attrib["v"]]
            try:
                value = int(period_element.text)
            except ValueError:
                value = 0
            energy_sum = result[int(period_element.attrib["periode"])].get(energy_type, 0)
            energy_sum += value
            result[int(period_element.attrib["periode"])][energy_type] = energy_sum
    return result


def parse_xml(xml_string: str) -> List[Dict]:
    """
    Parse the xml received from https://eco2mix.rte-france.com/curves/eco2mixWeb to a dict of dicts:
    e.g.: [{ "time": 234987239847, "country": "FR", "wind": 20000, "coal": 20000, ... }]
    Note: Country will be hardcoded to FR for now.
    """
    root = ET.fromstring(xml_string)
    result = []
    for day_index in range(7, len(root)):
        day_element = root[day_index]
        result += parse_xml_day(day_element)
    return result


def get_url_for_day(day: datetime.date) -> str:
    """
    Return the url to request the energy mix data for a single day from https://eco2mix.rte-france.com/curves/eco2mixWeb
    e.g. https://eco2mix.rte-france.com/curves/eco2mixWeb?type=mix&dateDeb=27/10/2022&dateFin=27/10/2022&mode=NORM
    """
    return f"https://eco2mix.rte-france.com/curves/eco2mixWeb?type=mix&dateDeb={day.strftime('%d/%m/%Y')}&dateFin={day.strftime('%d/%m/%Y')}&mode=NORM"


def write_values(client, reading):
    print(f"Writing values {reading}")

    dimensions = [
        {'Name': 'Country', 'Value': 'FR'}
    ]

    common_attributes = {
        'Dimensions': dimensions,
        'MeasureName': 'energy-mix',
        'MeasureValueType': 'MULTI'
    }

    records = []

    for r in reading: 
        record = {
            'Time': str(reading['time']*1000),
            'MeasureValues': []
        }

        nuclear = {
            'Name': 'nuclear',
            'Value': f"{reading['nuclear']}",
            'Type': 'BIGINT'
        }
        coal = {
            'Name': 'coal',
            'Value': f"{reading['coal']}",
            'Type': 'BIGINT'
        }
        gas = {
            'Name': 'gas',
            'Value': f"{reading['gas']}",
            'Type': 'BIGINT'
        }
        heavy_oil = {
            'Name': 'heavy-oil',
            'Value': f"{reading['heavy-oil']}",
            'Type': 'BIGINT'
        }
        oil = {
            'Name': 'oil',
            'Value': f"{reading['oil']}",
            'Type': 'BIGINT'
        }
        hydro = {
            'Name': 'hydro',
            'Value': f"{reading['hydro']}",
            'Type': 'BIGINT'
        }
        wind = {
            'Name': 'wind',
            'Value': f"{reading['wind']}",
            'Type': 'BIGINT'
        }
        others = {
            'Name': 'others',
            'Value': f"{reading['others']}",
            'Type': 'BIGINT'
        }
        pumped_storage = {
            'Name': 'pumped-storage',
            'Value': f"{reading['pumped-storage']}",
            'Type': 'BIGINT'
        }
        solar = {
            'Name': 'solar',
            'Value': f"{reading['solar']}",
            'Type': 'BIGINT'
        }
        consumption = {
            'Name': 'consumption',
            'Value': f"{reading['consumption']}",
            'Type': 'BIGINT'
        }

        record['MeasureValues'] = [nuclear, coal, gas, heavy_oil, oil, hydro, wind, others, pumped_storage, solar, consumption]
        records.append(record)

    try:
        result = client.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME,
                                        Records=records, CommonAttributes=common_attributes)

        print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
    except client.exceptions.RejectedRecordsException as err:
        print(f"Rejected: {err}")
        for rr in err.response["RejectedRecords"]:
            print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
        if "ExistingVersion" in rr:
            print("Rejected record existing version: ", rr["ExistingVersion"])
    except Exception as err:
        print("Error:", err)

def create_table(client):
        print("Creating table")
        retention_properties = {
            'MemoryStoreRetentionPeriodInHours': 12,
            'MagneticStoreRetentionPeriodInDays': 10
        }
        try:
            client.create_table(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME,
                                     RetentionProperties=retention_properties)
            print("Table [%s] successfully created." % TABLE_NAME)
        except client.exceptions.ConflictException:
            print("Table [%s] exists on database [%s]. Skipping table creation" % (
                TABLE_NAME, DATABASE_NAME))
        except Exception as err:
            print("Create table failed:", err)

def lambda_handler(event, context):
    xml = requests.get(
        f"https://eco2mix.rte-france.com/curves/eco2mixWeb?type=mix&dateDeb={event['startDate']}&dateFin={event['endDate']}&mode=NORM").text
    p = parse_xml(xml)
    print(p)

    session = boto3.Session()
    clientWrite = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000,
                                                              retries={'max_attempts': 10}))

    create_table(clientWrite)

    
    write_values(clientWrite, p)

    return {
        'statusCode': 200,
        'body': json.dumps('Job successful')
    }