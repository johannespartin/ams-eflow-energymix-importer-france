# e-flow energy mix importer

AWS Lambda function to write energy mix data to a Timestream DB. 

Event to be formatted as following: 

    {
    "startDate": "22/10/2022",
    "endDate": "28/10/2022"
    }
