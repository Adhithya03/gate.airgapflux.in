import mysql.connector
import concurrent.futures
import re
import time
from openai import OpenAI
from mysql.connector import pooling

# Subject topic mapping for Electrical Engineering
ee_subject_topics = {
    "Engineering Mathematics": {
        "Linear Algebra": "Matrix Algebra, Systems of linear equations, Eigenvalues, Eigenvectors.",
        "Calculus": "Mean value theorems, Theorems of integral calculus, Evaluation of definite and improper integrals, Partial Derivatives, Maxima and minima, Multiple integrals, Vector identities, Directional derivatives, Line integral, Surface integral, Volume integral, Stokes's theorem, Gauss's theorem, Divergence theorem, Green's theorem.",
        "Differential Equations": "First order equations (linear and nonlinear), Higher order linear differential equations with constant coefficients, Method of variation of parameters, Cauchy's equation, Euler's equation, Initial and boundary value problems, Partial Differential Equations, Method of separation of variables.",
        "Complex Variables": "Analytic functions, Cauchy's integral theorem, Cauchy's integral formula, Taylor series, Laurent series, Residue theorem, Solution integrals.",
        "Probability and Statistics": "Sampling theorems, Conditional probability, Mean, Median, Mode, Standard Deviation, Random variables, Discrete and Continuous distributions, Poisson distribution, Normal distribution, Binomial distribution, Correlation analysis, Regression analysis."
    },
    "Electric circuits": {
        "Network Elements": "Voltage and Current sources, dependent sources, R, L, C, M elements.",
        "Network Theorems": "Thevenin, Norton, Superposition, and Maximum Power Transfer theorems.",
        "Transient Response": "Transient response of DC and AC networks.",
        "Sinusoidal Steady-State Analysis": "Sinusoidal steady-state analysis.",
        "Resonance": "Resonance in AC networks.",
        "Two Port Networks": "Analysis and applications of two port networks.",
        "Complex Power and Power Factor": "Complex power calculations and power factor in AC circuits."
    },
    "Electromagnetic Fields": {
        "Electric Field Intensity": "Electric field intensity for various charge distributions.",
        "Electric Flux Density": "Electric flux density and Gauss's Law applications.",
        "Divergence": "Divergence in vector calculus for electric fields.",
        "Electric Potential": "Electric field and potential due to point, line, plane, and spherical charge distributions.",
        "Capacitance": "Capacitance of simple configurations.",
        "Curl": "Curl in vector calculus for magnetic fields.",
        "Inductance": "Self and mutual inductance concepts.",
        "Magnetic Circuits": "Magnetomotive force, Reluctance, and magnetic circuit analysis."
    },
    "Signals and Systems": {
        "Signal Properties": "Shifting and scaling properties of signals.",
        "LTI Systems": "Linear time-invariant and causal systems analysis.",
        "Fourier Series": "Fourier series representation for periodic signals.",
        "Sampling Theorem": "Nyquist-Shannon sampling theorem.",
        "Fourier Transform": "Applications of Fourier Transform in signal analysis.",
        "Laplace and Z Transforms": "Laplace Transform and Z transform techniques.",
        "RMS and Average Values": "RMS and average value calculations for periodic waveforms."
    },
    "Electrical Machines": {
        "Transformers": "Auto-Transformer: Principles and applications of autotransformer, Three Phase Transformers: Connections, vector groups, and parallel operation, Single Phase Transformer Equivalent circuit, open/short circuit tests, regulation, and efficiency",
        "Electromechanical Conversion": "Electromechanical energy conversion principles.",
        "DC Machines": "Separately excited, series, and shunt DC machines, characteristics, and speed control, efficiency and loss",
        "Three Phase Induction Machines": "Principle of operation, torque-speed characteristics, equivalent circuit, and speed control, efficiency",
        "Single Phase Induction Motors": "Operating principles of single-phase induction motors.",
        "Synchronous Machines": "Cylindrical and salient pole machines, performance, regulation, and starting methods, efficiency",
    },
    "Power Systems": {
        "Transmission Concepts": "AC and DC transmission models and performance. Compensation: Series and shunt compensation techniques",
        "Economic Load Dispatch": "Economic Load Dispatch with and without transmission losses, Basic concepts of electrical power generation.",
        "Insulators and Distribution Systems": "Electric field distribution and insulator design Analysis and design of distribution systems.",
        "Load Flow Methods": "Gauss-Seidel and Newton-Raphson load flow methods.",
        "Voltage/Frequency Control": "Voltage and frequency regulation in power systems.",
        "Power Factor Correction": "Techniques for power factor improvement.",
        "Fault Analysis": "Symmetrical and unsymmetrical fault analysis and Symmetrical components for fault analysis.",
        "Protection Systems": "Over-current, differential, directional, and distance protection, Circuit Breakers Operation and types of circuit breakers.",
        "System Stability": "Stability concepts and equal area criterion, Swing equation, Critical clearing angle and time."
    },
    "Control Systems": {
        "Block Diagrams/Signal Flow": "Block diagrams and Signal flow graphs.",
        "System Analysis": "Transient and steady-state analysis of LTI systems.",
        "Stability Criteria": "Routh-Hurwitz and Nyquist stability criteria.",
        "Frequency Response": "Bode plots and root locus analysis.",
        "Compensators and Controllers": "Lag, Lead, and Lead-Lag compensators and P, PI, and PID controllers.",
        "State Space Analysis": "State space models and solution of state equations."
    },
    "Electrical and Electronic Measurements": {
        "Bridges/Potentiometers and Instrument tranformers": "Bridges and potentiometers for measurements. Current and voltage transformers",
        "Meters": "Measurement of voltage, current, power, energy, and power factor.",
        "Phase/Time/Frequency Oscilloscopes": "Operation and applications of oscilloscopes, Phase, time, and frequency measurement methods",
        "Error Analysis": "Error analysis in measurements."
    },
    "Analog Electronics": {
        "Diode Circuits": "Clipping, clamping, and rectifier circuits.",
        "Amplifiers": "Biasing, equivalent circuits, and frequency response.",
        "Oscillators": "Feedback amplifiers and oscillator circuits. VCOs/Timers: Voltage-controlled oscillators and timers",
        "Op-Amps": "Operational amplifier characteristics and applications, Single-stage active filters, Active Filters: Sallen Key, and Butterworth filters",
    },
    "Digital Electronics": {
        "Combinational Logic": "Combinatorial and Multiplexers and demultiplexers.",
        "Sequential Circuits": "sequential logic circuits.",
        "AD/DA Converters": "A/D and D/A converters. Schmitt trigger circuits."
    },
    "Power Electronics": {
        "Power Semiconductor Devices": "Static V-I characteristics and firing circuits for Thyristor, MOSFET, IGBT.",
        "DC-DC Converters": "Buck, Boost, and Buck-Boost Converters.",
        "Rectifiers": "Single and three-phase uncontrolled rectifiers.",
        "Thyristor Converters": "Voltage and current commutated Thyristor-based converters.",
        "AC-DC Converters": "Bidirectional AC to DC voltage source converters.",
        "Harmonics and Power Factor": "Harmonic analysis and distortion factor in converters.",
        "Inverters": "Single-phase and three-phase voltage/current source inverters.",
        "PWM Techniques": "Sinusoidal pulse width modulation."
    }
}

# Define GA subject topics - placeholder topics for General Aptitude
ga_subject_topics = {
    "Verbal Aptitude": {
        "English Grammar": "Basic grammar rules, parts of speech, sentence construction.",
        "Vocabulary": "Word meanings, synonyms, antonyms, analogies.",
        "Reading Comprehension": "Understanding passages, inference drawing, author's intent.",
        "Critical Reasoning": "Argument analysis, assumption identification, logical deduction."
    },
    "Quantitative Aptitude": {
        "Number Systems": "Integers, fractions, decimals, properties of numbers.",
        "Arithmetic": "Percentages, ratios, averages, profit and loss, time and work.",
        "Algebra": "Linear equations, quadratic equations, polynomials.",
        "Geometry": "Lines, angles, triangles, circles, coordinate geometry.",
        "Calculus": "Derivatives, integrals, applications."
    },
    "Analytical Aptitude": {
        "Data Interpretation": "Tables, charts, graphs, data analysis.",
        "Logical Reasoning": "Deductive and inductive reasoning, analogies, syllogisms.",
        "Pattern Recognition": "Numerical and visual pattern recognition."
    },
    "Spatial Aptitude": {
        "Spatial Visualization": "Mental rotation, spatial orientation.",
        "Spatial Reasoning": "Paper folding, pattern completion, block diagrams."
    }
}

# Database connection details
DB_CONFIG = {
    'host': '',
    'user': '',
    'password': '',
    'database': '',
}

# API configuration
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
    """Get records from database where subject IS NOT NULL but topic IS NULL"""
    conn = None
    try:
        conn = get_connection_from_pool()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM PYQ WHERE subject IS NOT NULL AND topic IS NULL")
        records = cursor.fetchall()
        cursor.close()
        return records
    except mysql.connector.Error as e:
        print(f"Error retrieving records: {e}")
        return []
    finally:
        if conn:
            conn.close()

def update_topic(year, page_number, question_number, topic):
    """Update the topic field for a specific record"""
    conn = None
    try:
        conn = get_connection_from_pool()
        if not conn:
            print("Failed to get database connection from pool")
            return False

        cursor = conn.cursor()
        query = """UPDATE PYQ SET topic = %s
                   WHERE year = %s AND page_number = %s AND question_number = %s"""
        params = (topic, year, page_number, question_number)
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

def validate_xml_response(response, topics):
    """Validate XML response and extract topic"""
    try:
        match = re.search(r'<topic>(.*?)</topic>', response, re.DOTALL)
        if not match:
            return None

        topic = match.group(1).strip()

        # Check if the topic is in the list of valid topics
        if topic in topics:
            return topic

        return None
    except Exception as e:
        print(f"Error validating XML: {e}")
        return None

def query_deepseek(system_prompt, question_prompt):
    """Query the API"""
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
    """Construct prompt based on record data"""
    subject = record['subject']

    # Get the topics for this subject
    subject_topics = None
    if record['section'] == 'EE' and subject in ee_subject_topics:
        subject_topics = ee_subject_topics[subject]
    elif record['section'] == 'GA' and subject in ga_subject_topics:
        subject_topics = ga_subject_topics[subject]

    if not subject_topics:
        print(f"Unknown subject or section: {subject} in {record['section']}")
        return None, None, []

    topics = list(subject_topics.keys())
    topic_descriptions = [f"{topic}: {desc}" for topic, desc in subject_topics.items()]

    # System prompt with strict instructions
    system_prompt = f"""You are an extremely precise classifier for GATE exam questions.
Your task is to determine the single, most appropriate topic for this {subject} question.
Choose from the following topics:
{', '.join(topics)}

Here are descriptions of each topic:
{chr(10).join(topic_descriptions)}

IMPORTANT:
1. You MUST choose exactly one topic from the list above.
2. Your response MUST be ONLY the chosen topic enclosed in XML tags.
3. The required format is EXACTLY:
    <topic>Your Chosen Topic</topic>
No extra whitespace, punctuation, text, explanations, or line breaks are allowed.
For example, if the correct topic is "Linear Algebra", your entire response must be:
<topic>Linear Algebra</topic>
Now, classify the following question:
"""

    # Question prompt with the actual question content
    question_prompt = f"Question: {record['question_text']}\n"

    # If the question has options, add them
    if record['question_type'] in ['MCQ', 'MSQ', 'MTA'] and record['option_a']:
        question_prompt += f"Option A: {record['option_a']}\n"
        question_prompt += f"Option B: {record['option_b']}\n"
        question_prompt += f"Option C: {record['option_c']}\n"
        question_prompt += f"Option D: {record['option_d']}\n"

    # If there's an image description available, include it
    if record['has_diagram'] and record['image_description']:
        question_prompt += f"Image Description: {record['image_description']}\n"

    return system_prompt, question_prompt, topics

def process_record(record):
    """Process a single record with retry logic"""
    max_retries = 5
    retry_count = 0

    while retry_count < max_retries:
        # Construct prompt
        system_prompt, question_prompt, topics = construct_prompt(record)

        if system_prompt is None or not topics:
            print(f"No topics defined for subject: {record['subject']} in section: {record['section']}")
            return record, None

        record_identifier = f"year={record['year']}, page={record['page_number']}, question={record['question_number']}"

        # Query the model
        try:
            response = query_deepseek(system_prompt, question_prompt)
        except Exception as e:
            print(f"API call failed for record {record_identifier}: {e}")
            response = None  # Set response to None to force a retry

        if response:
            # print(f"Raw API response for {record_identifier}:\n{response}")  # Log the raw response

            # Validate the response
            topic = validate_xml_response(response, topics)

            if topic:
                return record, topic
            else:
                print(f"Invalid XML or topic for {record_identifier}. Response: {response}")
        else:
            print(f"API returned None (likely an error) for {record_identifier}.")

        retry_count += 1
        print(f"Retry {retry_count}/{max_retries} for record {record_identifier}")
        time.sleep(2)  # Increased delay between retries

    print(f"Failed to classify record {record_identifier} after {max_retries} retries")
    return record, None

def worker_function(records, worker_id):
    """Worker function to process a batch of records"""
    processed_count = 0
    total_count = len(records)

    print(f"Worker {worker_id}: Started processing {total_count} records.")

    for record in records:
        # Process the record
        _, topic = process_record(record)

        if topic:
            # Update the database - using a fresh connection each time
            success = update_topic(record['year'], record['page_number'], record['question_number'], topic)

            if success:
                processed_count += 1
            else:
                # If update fails, wait and retry once
                time.sleep(2)
                success = update_topic(record['year'], record['page_number'], record['question_number'], topic)
                if success:
                    processed_count += 1
                    # print(f"Worker {worker_id}: Updated record on retry: year={record['year']}, page={record['page_number']}, question={record['question_number']} with topic={topic}")

        # Small delay between records to prevent overwhelming the database
        time.sleep(0.3)

    print(f"Worker {worker_id}: Completed. Processed {processed_count}/{total_count} records.")
    return processed_count

def main():
    """Main function to orchestrate the topic classification process"""
    # Initialize the connection pool
    init_connection_pool()

    # Get unclassified records
    records = get_unclassified_records()

    if not records:
        print("No records found with subject but without topic. Exiting.")
        return

    total_records = len(records)
    print(f"Found {total_records} records with subject but without topic.")

    # Define number of workers
    num_workers = 10

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
    print(f"Topic classification completed. Successfully processed {total_processed}/{total_records} records.")

if __name__ == "__main__":
    main()
