import mysql.connector
import concurrent.futures
import re
import time
from openai import OpenAI
from mysql.connector import pooling

# Subject lists
ee_subjects = [
    "Engineering Mathematics",
    "Electric circuits",
    "Electromagnetic Fields",
    "Signals and Systems",
    "Electrical Machines",
    "Power Systems",
    "Control Systems",
    "Electrical and Electronic Measurements",
    "Analog Electronics",
    "Digital Electronics",
    "Power Electronics"
]

ga_subjects = [
    "Verbal Aptitude",
    "Quantitative Aptitude",
    "Analytical Aptitude",
    "Spatial Aptitude"
]

# Database connection details - replace with your actual credentials
DB_CONFIG = {
    'host': '',
    'user': '',
    'password': '',
    'database': '',
}

# DeepSeek API configuration
PROVIDER_CONFIG = {
    "base_url": "https://openrouter.ai/api/v1",
    "api_key": "",
    "model": "meta-llama/llama-3.1-70b-instruct",
}

# Create a connection pool
connection_pool = None

def init_connection_pool():
    """Initialize connection pool"""
    global connection_pool
    try:
        connection_pool = pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=10,
            **DB_CONFIG
        )
        print("Connection pool created successfully")
    except Exception as e:
        print(f"Error creating connection pool: {e}")

def get_connection_from_pool():
    """Get a connection from the pool"""
    global connection_pool
    try:
        return connection_pool.get_connection()
    except Exception as e:
        print(f"Error getting connection from pool: {e}")
        return None

def get_unclassified_records():
    """Get records from database where subject IS NULL"""
    conn = None
    try:
        conn = get_connection_from_pool()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM PYQ WHERE subject IS NULL")
        records = cursor.fetchall()
        cursor.close()
        return records
    except mysql.connector.Error as e:
        print(f"Error retrieving records: {e}")
        return []
    finally:
        if conn:
            conn.close()

def update_subject(year, page_number, question_number, subject):
    """Update the subject field for a specific record with a fresh connection"""
    conn = None
    try:
        conn = get_connection_from_pool()
        if not conn:
            print("Failed to get database connection from pool")
            return False

        cursor = conn.cursor()
        query = """UPDATE PYQ SET subject = %s
                   WHERE year = %s AND page_number = %s AND question_number = %s"""
        params = (subject, year, page_number, question_number)
        cursor.execute(query, params)
        conn.commit()
        cursor.close()
        return True
    except mysql.connector.Error as e:
        print(f"Error updating record: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def validate_xml_response(response, subjects):
    """Validate XML response and extract subject"""
    try:
        match = re.search(r'<subject>(.*?)</subject>', response, re.DOTALL)
        if not match:
            return None

        subject = match.group(1).strip()

        # Check if the subject is in the list of valid subjects
        if subject in subjects:
            return subject

        return None
    except Exception as e:
        print(f"Error validating XML: {e}")
        return None

def query_deepseek(system_prompt, question_prompt):
    """Query the DeepSeek API"""
    try:
        client = OpenAI(
            base_url=PROVIDER_CONFIG.get("base_url"),
            api_key=PROVIDER_CONFIG.get("api_key"),
        )

        completion = client.chat.completions.create(
            model=PROVIDER_CONFIG.get("model"),
            messages=[
                {
                    "role": "user",
                    "content": system_prompt + question_prompt
                }
            ],
            temperature=0.5,
        )

        return completion.choices[0].message.content
    except Exception as e:
        print(f"Error querying API: {e}")
        time.sleep(2)  # Add delay on API error
        return None
def construct_prompt(record):
    """Construct prompt based on record data with strict formatting instructions."""
    # Select the appropriate subject list based on the question's section.
    subjects = ee_subjects if record['section'] == 'EE' else ga_subjects

    # System prompt with strict instructions.
    system_prompt = f"""You are an extremely precise classifier for GATE exam questions.
Your task is to determine the single, most appropriate subject from the following list:
{', '.join(subjects)}
IMPORTANT:
1. You MUST choose exactly one subject from the list above.
2. Your response MUST be ONLY the chosen subject enclosed in XML tags.
3. The required format is EXACTLY:
    <subject>Your Chosen Subject</subject>
No extra whitespace, punctuation, text, explanations, or line breaks are allowed.
For example, if the correct subject is Electrical Machines, your entire response must be:
<subject>Electrical Machines</subject>
Now, classify the following question:
"""

    # Question prompt with the actual question content.
    question_prompt = f"Question: {record['question_text']}\n"

    # If the question has options, add them.
    if record['question_type'] in ['MCQ', 'MSQ', 'MTA'] and record['option_a']:
        question_prompt += f"Option A: {record['option_a']}\n"
        question_prompt += f"Option B: {record['option_b']}\n"
        question_prompt += f"Option C: {record['option_c']}\n"
        question_prompt += f"Option D: {record['option_d']}\n"

    # If there's an image description available, include it.
    if record['has_diagram'] and record['image_description']:
        question_prompt += f"Image Description: {record['image_description']}\n"

    return system_prompt, question_prompt, subjects

def process_record(record):
    """Process a single record with retry logic and detailed logging."""
    max_retries = 5
    retry_count = 0

    while retry_count < max_retries:
        # Construct prompt
        system_prompt, question_prompt, subjects = construct_prompt(record)
        record_identifier = f"year={record['year']}, page={record['page_number']}, question={record['question_number']}"
        # print(f"Processing record: {record_identifier}")

        # Query the model
        try:
            response = query_deepseek(system_prompt, question_prompt)
        except Exception as e:
            print(f"API call failed for record {record_identifier}: {e}")
            response = None  # Set response to None to force a retry

        if response:
            print(f"Raw API response for {record_identifier}:\n{response}")  # Log the raw response

            # Validate the response
            subject = validate_xml_response(response, subjects)

            if subject:
                # print(f"Successfully classified {record_identifier} as: {subject}")
                return record, subject
            else:
                print(f"Invalid XML or subject for {record_identifier}. Response: {response}")
        else:
                print(f"API returned None (likely an error) for {record_identifier}.")

        retry_count += 1
        print(f"Retry {retry_count}/{max_retries} for record {record_identifier}")
        time.sleep(2)  # Increased delay: API calls can be slow

    print(f"Failed to classify record {record_identifier} after {max_retries} retries")
    return record, None

def worker_function(records, worker_id):
    """Worker function to process a batch of records"""
    processed_count = 0
    total_count = len(records)

    print(f"Worker {worker_id}: Started processing {total_count} records.")

    for record in records:
        # Process the record
        _, subject = process_record(record)

        if subject:
            # Update the database - using a fresh connection each time
            success = update_subject(record['year'], record['page_number'], record['question_number'], subject)

            if success:
                processed_count += 1
                # print(f"Worker {worker_id}: Updated record year={record['year']}, page={record['page_number']}, question={record['question_number']} with subject={subject}")
            else:
                # If update fails, wait and retry once
                time.sleep(2)
                success = update_subject(record['year'], record['page_number'], record['question_number'], subject)
                if success:
                    processed_count += 1
                    print(f"Worker {worker_id}: Updated record on retry: year={record['year']}, page={record['page_number']}, question={record['question_number']} with subject={subject}")

        # Small delay between records to prevent overwhelming the database
        time.sleep(0.3)

    print(f"Worker {worker_id}: Completed. Processed {processed_count}/{total_count} records.")
    return processed_count

def main():
    """Main function to orchestrate the classification process"""
    # Initialize the connection pool
    init_connection_pool()

    # Get unclassified records
    records = get_unclassified_records()

    if not records:
        print("No unclassified records found. Exiting.")
        return

    total_records = len(records)
    print(f"Found {total_records} unclassified records.")

    # Define number of workers (reduced to avoid overwhelming the connection pool)
    num_workers = 20

    # Split records among workers
    records_per_worker = (total_records + num_workers - 1) // num_workers
    worker_batches = []

    for i in range(0, total_records, records_per_worker):
        batch = records[i:min(i+records_per_worker, total_records)]
        if batch:
            worker_batches.append(batch)

    # Process records using multiple workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker_function, batch, i) for i, batch in enumerate(worker_batches)]

        # Wait for all workers to complete
        processed_counts = [future.result() for future in concurrent.futures.as_completed(futures)]

    total_processed = sum(processed_counts)
    print(f"Classification completed. Successfully processed {total_processed}/{total_records} records.")

if __name__ == "__main__":
    main()
