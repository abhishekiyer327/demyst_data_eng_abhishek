"""
All the required functions are defined in section 1 below
The actual script is just calling the functions in section 2
"""

# Importing all the required libraries
import hashlib
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

""""
Section 1 - Functions
"""

# Function to generate a csv file
def genrate_csv_file():

    # Saving the headers in a variable
    header = "first_name,last_name,address,date_of_birth\n"

    
    # Creating some sample data for creating the csv file
    data = [
        ("John", "Doe", "123 Main St", "1985-04-12"),
        ("Jane", "Smith", "456 Maple Ave", "1990-07-23"),
        ("Emily", "Johnson", "789 Oak Rd", "1995-03-15"),
        ("Jack", "Ryan", "27 W. 25th Street, 6th 9th FL New York, NY 10001", "1997-03-27"),
        ("Sherlock", "Homles", "221B Baker Street, London", "1854-01-06")
    ]


    # Creating the file and opening in write mode
    with open("data.csv", "w") as file:
        # Writing the header
        file.write(header)

        #Writing the data
        for row in data:
            line = ','.join(f'"{field}"' for field in row) + "\n"
            file.write(line)


def generate_salt(length=16):
    """Return a salt value which can be changed here in the variable whenever needed"""
    salt_value = "abhisheks_salt"
    return salt_value
    

def hash_value(value, salt):
    """Hashes the value with the provided salt using SHA-256 and returns the hex digest."""
    salted_value = f"{salt}{value}".encode()
    return hashlib.sha256(salted_value).hexdigest()


def anonymize_data(input_file, output_file, columns_to_hash):
    """Anonymizes specified columns in a CSV file by hashing their values with a salt."""
    
    with open(input_file, "r", newline='') as infile, open(output_file, "w", newline='') as outfile:
        # Read the header from the original file
        header = infile.readline().strip()
        header_fields = header.split(',')
        outfile.write(header + "\n")  # Write the same header to the new file

        # Store the salt value
        salt = generate_salt()

        # Determine column indices to hash
        indices_to_hash = [header_fields.index(col) for col in columns_to_hash if col in header_fields]
        
        # Process each row in the original file
        for line in infile:
            # Handle quoted fields with commas
            fields = []
            current_field = ''
            inside_quotes = False
            for char in line:
                if char == ',' and not inside_quotes:
                    fields.append(current_field.strip('"'))
                    current_field = ''
                elif char == '"':
                    inside_quotes = not inside_quotes
                else:
                    current_field += char
            fields.append(current_field.strip('"'))

            # Hash the specified columns with the salt
            for index in indices_to_hash:
                fields[index] = hash_value(fields[index], salt)
            
            # Join the hashed fields back into a single line, respecting quotes
            hashed_line = ','.join(f'"{field}"' if ',' in field or '"' in field else field for field in fields)
            outfile.write(hashed_line)


def anonymize_data_with_spark(input_file, output_file, columns_to_hash):
    """Anonymizes specified columns in a large CSV file using PySpark"""
    spark = SparkSession.builder \
        .appName("Large CSV Anonymization") \
        .getOrCreate()

    # Load CSV data
    df = spark.read.csv(input_file, header=True, inferSchema=True)

    # Generate salt
    salt = generate_salt()

    # Define the UDF to hash values
    def hash_udf(value):
        return hash_value(value, salt)
    
    hash_udf_spark = udf(hash_udf, StringType())

    # Apply hashing to the specified columns
    for column in columns_to_hash:
        if column in df.columns:
            df = df.withColumn(column, hash_udf_spark(df[column]))

    # Save the anonymized data
    df.write.csv(output_file, header=True)

    # Stop the SparkSession
    spark.stop()




"""
Section 2 - Actual Script
"""


# Calling the function to create a csv file
genrate_csv_file()


# Calling the function to anonymize the fiels
anonymize_data("data.csv", "hashed_data.csv", ["first_name", "last_name", "address"])

# Calling the function to anonymize the fields using PySpark
anonymize_data_with_spark("data.csv", "hashed_data_spark.csv", ["first_name", "last_name", "address"])