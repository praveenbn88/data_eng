import re

from sample_s3_paths import *
import logging 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

## This function returns all the S3 paths that match the pattern
def find_s3_bucket_paths(script_path):
    # Regular expression pattern to match S3 bucket paths
    logger.info(f"started find_s3_bucket_paths with arg {find_s3_bucket_paths}")
    pattern = r"s3[a]?:\/\/[a-zA-Z0-9\-_]+" ##r"(['\"]s3[a]?:\/\/[a-zA-Z0-9\-_]+['\"])(\/[a-zA-Z0-9\-_\/]+)*"

    # Dictionary to store variable names and their corresponding values
    variables = {}

    # List to store lines containing S3 bucket paths
    lines_with_s3_paths = []

    # Function to resolve variable values recursively
    def resolve_variable_value(var_name):
        if var_name in variables:
            value = variables[var_name]
            if re.match(pattern, value):
                #lines_with_s3_paths.append(f"{var_name} = {value}")
                pass
            else:
                #resolve_variable_value(value)
                pass

    # Open the script file and read its content line by line
    with open(script_path, 'r') as file:
        for line in file:
            # Find all occurrences of variable assignments using regular expression
            matches = re.findall(r"([a-zA-Z0-9_]+)\s*=\s*[f]?(['\"].*?['\"])", line)
            #print(f"These are variable associations {matches}")
            for var_name, value in matches:
                variables[var_name] = value

            # Find all occurrences of S3 bucket paths using regular expression
            matches = re.findall(pattern, line)
            for match in matches:
                bucket_path = match[0]
                lines_with_s3_paths.append(line.strip())
                for var_name in variables.keys():
                    if var_name in line:
                        resolve_variable_value(var_name)

        key=0
        val=0
        new_line=0
        for key,val in globals().items():
            new_line = f"{key}={val}"
            matches = re.findall(pattern, new_line)
            for match in matches:
                lines_with_s3_paths.append(new_line.strip())





    return lines_with_s3_paths

if __name__ == "__main__":
    # Path to the Python script to scan
    script_path = "sample_s3_paths.py"

    # Find all lines containing S3 bucket paths in the script
    lines_with_s3_paths = find_s3_bucket_paths(script_path)

    # Print the lines containing S3 bucket paths
    print("Lines containing S3 Bucket Paths:")
    for line in lines_with_s3_paths:
        if line.startswith("#") or line.startswith("'''"):
            continue
        print(line)


