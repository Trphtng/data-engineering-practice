# Use an official Python base image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy all files into the container
COPY . /app

# Install any necessary dependencies (none needed in this case, unless you use boto3 or others)
RUN pip install --no-cache-dir boto3

# Set the default command to run the app
CMD ["python", "main.py"]
