import os

BUCKET = "global-commerce-data"

def is_aws_configured():
    return (
        os.getenv("AWS_ACCESS_KEY_ID") is not None and
        os.getenv("AWS_SECRET_ACCESS_KEY") is not None
    )

def bronze_path():
    if is_aws_configured():
        return f"s3a://{BUCKET}/bronze/"
    return "/home/vicky/global-commerce-intelligence-platform/data/bronze/"

def silver_path():
    if is_aws_configured():
        return f"s3a://{BUCKET}/silver/"
    return "/home/vicky/global-commerce-intelligence-platform/data/silver/"

def gold_path():
    if is_aws_configured():
        return f"s3a://{BUCKET}/gold/"
    return "/home/vicky/global-commerce-intelligence-platform/data/gold/"